"use strict";

import {
  isBelowThreshold,
  settleRideTransaction,
  type SettleRideParams,
} from "./ledger.service.js";
import logger from "../config/logger.js";
import {
  parseTransactionBatch,
  buildDeltaCommand,
  type ParsedTransaction,
} from "../utils/parser.js";
import { broadcastDeltaToFleet } from "./sync.service.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";

type LoggerInstance = typeof logger;

// Ingests a batch of transaction payloads from a terminal, validates each row, and forwards valid transactions to the backend for settlement. Invalid rows are logged and acknowledged with an error response to the terminal.
async function ingestTransactionBatch(
  terminalId: string,
  rawPayload: string
): Promise<void> {
  const log = logger.child({ terminalId });
  log.info({ payloadLength: rawPayload.length }, "ingestion.batch_received");

  const { valid, invalid } = parseTransactionBatch(rawPayload, terminalId);

  // Error Isolation: structured error logging for malformed rows
  if (invalid.length > 0) {
    log.warn(
      { invalidRows: invalid, terminalId },
      "ingestion.invalid_rows_detected"
    );
    for (const item of invalid) {
      const firstToken = item.line.split(",")[0]?.trim();
      if (firstToken && firstToken.length > 0 && !firstToken.includes(":")) {
        publishToTerminal(
          terminalId,
          `ACK:FAIL,${firstToken},MALFORMED_PAYLOAD`
        ).catch((err) => {
          log.error({ err }, "ingestion.invalid_row_ack_failed");
        });
      }
    }
  }

  if (valid.length === 0) {
    log.warn("ingestion.no_valid_rows");
    return;
  }

  // Process each valid transaction independently with isolated error boundary
  for (const tx of valid) {
    await processTransaction(tx, terminalId, log);
  }

  log.info({ processedCount: valid.length }, "ingestion.batch_complete");
}

// Processes a single transaction, forwards it to the backend for settlement, and handles the response by sending an appropriate ACK to the terminal. Errors are logged and result in a SERVER_ERROR ACK.
async function processTransaction(
  tx: ParsedTransaction,
  terminalId: string,
  log: LoggerInstance
): Promise<void> {
  const effectiveCardUid = tx.card_uid || tx.student_uid;
  const effectiveTerminalId = tx.terminal_id || terminalId;

  const txLog = log.child({
    transactionId: tx.transaction_id,
    cardUid: effectiveCardUid,
    terminalId: effectiveTerminalId,
    location: tx.location,
    driverUid: tx.driver_uid,
  });

  try {
    const backendPayload: SettleRideParams = {
      protocolVersion: tx.location ? "v1.1.0L" : undefined,
      terminalId: effectiveTerminalId,
      studentUid: effectiveCardUid,
      cardUid: effectiveCardUid,
      rawAmount: tx.raw_amount,
      amount: tx.amount,
      timestamp: tx.timestamp,
      driverUid: tx.driver_uid || null,
      location: tx.location || null,
      transactionId: tx.transaction_id,
      syncedAt: tx.synced_at,
    };

    const result = await settleRideTransaction(backendPayload);

    switch (result.status) {
      case "SUCCESS": {
        const balanceStr = (result.newBalance ?? 0).toFixed(2);
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:OK,${effectiveCardUid},${balanceStr}`
        );
        txLog.info(
          { newBalance: result.newBalance, cardUid: effectiveCardUid },
          "ingestion.ack_sent_to_terminal"
        );

        // Post-deduction blacklist broadcast to terminal fleet
        if (
          result.belowThreshold ||
          (result.newBalance !== null && isBelowThreshold(result.newBalance))
        ) {
          const blacklistCmd = buildDeltaCommand("ADD", "BL", effectiveCardUid);
          txLog.info(
            { newBalance: result.newBalance, cardUid: effectiveCardUid },
            "ingestion.balance_now_below_threshold — broadcasting blacklist delta"
          );

          process.nextTick(async () => {
            try {
              await broadcastDeltaToFleet(blacklistCmd);
              txLog.info(
                { blacklistCmd },
                "ingestion.blacklist_broadcast_sent"
              );
            } catch (err) {
              const errMsg =
                err instanceof Error ? err.message : "Unknown error";
              txLog.error(
                { err: errMsg },
                "ingestion.post_deduction_blacklist_broadcast_failed"
              );
            }
          });
        }
        break;
      }

      case "DUPLICATE": {
        txLog.info(
          { transactionId: result.transactionId || tx.transaction_id },
          "ingestion.duplicate_transaction_acknowledged"
        );
        // ACK with current balance so terminal clears retry queue without double charging
        const balanceStr = (result.newBalance ?? 0).toFixed(2);
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:OK,${effectiveCardUid},${balanceStr}`
        );
        break;
      }

      case "INSUFFICIENT_BALANCE": {
        txLog.warn(
          { cardUid: effectiveCardUid, fare: tx.amount },
          "ingestion.insufficient_balance"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},INSUFFICIENT_BALANCE`
        );
        break;
      }

      case "BLACKLISTED": {
        txLog.warn(
          { cardUid: effectiveCardUid },
          "ingestion.student_is_blacklisted"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},BLACKLISTED`
        );
        break;
      }

      case "CARD_NOT_REGISTERED": {
        txLog.warn(
          { cardUid: effectiveCardUid },
          "ingestion.card_uid_not_registered"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},CARD_NOT_REGISTERED`
        );
        break;
      }

      case "NOT_WHITELISTED":
      case "NOT_FOUND": {
        txLog.warn(
          { cardUid: effectiveCardUid },
          "ingestion.student_not_whitelisted"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},NOT_WHITELISTED`
        );
        break;
      }

      case "DRIVER_MISMATCH": {
        txLog.warn(
          {
            cardUid: effectiveCardUid,
            driverUid: tx.driver_uid,
            terminalId: effectiveTerminalId,
            reason: "DRIVER_MISMATCH",
          },
          "ingestion.driver_mismatch_rejected"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},DRIVER_MISMATCH`
        );
        break;
      }

      default: {
        txLog.error(
          { status: result.status },
          "ingestion.unhandled_settlement_status"
        );
        await publishToTerminal(
          effectiveTerminalId,
          `ACK:FAIL,${effectiveCardUid},SERVER_ERROR`
        );
        break;
      }
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    txLog.error({ err: errMsg }, "ingestion.transaction_processing_error");
    try {
      await publishToTerminal(
        effectiveTerminalId,
        `ACK:FAIL,${effectiveCardUid},SERVER_ERROR`
      );
    } catch (ackErr) {
      const ackErrMsg =
        ackErr instanceof Error ? ackErr.message : "Unknown error";
      txLog.error({ err: ackErrMsg }, "ingestion.ack_send_failed");
    }
  }
}

export { ingestTransactionBatch };
