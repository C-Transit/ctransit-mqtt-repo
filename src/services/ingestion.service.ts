// src/services/ingestion.service.ts
"use strict";

import {
  isBelowThreshold,
  settleRideTransaction,
  type SettleRideResult,
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

const errText = (err: unknown): string =>
  err instanceof Error ? err.message : "Unknown error";

// Ack for a settlement result. null = send nothing, so the terminal keeps the tap and resends.
function ackFor(result: SettleRideResult, cardUid: string): string | null {
  switch (result.status) {
    case "SUCCESS":
    case "DUPLICATE":
      // A duplicate gets the same OK so the terminal clears its retry queue.
      return `ACK:OK,${cardUid},${(result.newBalance ?? 0).toFixed(2)}`;
    case "INSUFFICIENT_BALANCE":
      return `ACK:FAIL,${cardUid},INSUFFICIENT_BALANCE`;
    case "BLACKLISTED":
      return `ACK:FAIL,${cardUid},BLACKLISTED`;
    case "CARD_NOT_REGISTERED":
      return `ACK:FAIL,${cardUid},CARD_NOT_REGISTERED`;
    case "NOT_WHITELISTED":
      return `ACK:FAIL,${cardUid},NOT_WHITELISTED`;
    case "DRIVER_MISMATCH":
      return `ACK:FAIL,${cardUid},DRIVER_MISMATCH`;
    case "INVALID_REQUEST":
      return `ACK:FAIL,${cardUid},MALFORMED_PAYLOAD`;
    default:
      return null;
  }
}

async function sendAck(
  terminalId: string,
  ack: string,
  log: LoggerInstance
): Promise<void> {
  try {
    await publishToTerminal(terminalId, ack);
  } catch (err) {
    // The charge (if any) is already committed. The terminal will resend and get a DUPLICATE OK.
    log.error({ err: errText(err) }, "ingestion.ack_publish_failed");
  }
}

// Ingests one tap line from a terminal. terminalId is the MQTT topic identity.
async function ingestTransactionBatch(
  terminalId: string,
  rawPayload: string
): Promise<void> {
  const log = logger.child({ terminalId });
  log.info({ payloadLength: rawPayload.length }, "ingestion.batch_received");

  const { valid, invalid } = parseTransactionBatch(rawPayload, terminalId);

  if (invalid.length > 0) {
    log.warn(
      { invalidCount: invalid.length, reasons: invalid.map((i) => i.reason) },
      "ingestion.invalid_rows_detected"
    );
    for (const item of invalid) {
      const firstToken = item.line.split(",")[0]?.trim();
      if (firstToken && firstToken.length > 0 && !firstToken.includes(":")) {
        publishToTerminal(
          terminalId,
          `ACK:FAIL,${firstToken},MALFORMED_PAYLOAD`
        ).catch((err) => {
          log.error({ err: errText(err) }, "ingestion.invalid_row_ack_failed");
        });
      }
    }
  }

  if (valid.length === 0) {
    log.warn("ingestion.no_valid_rows");
    return;
  }

  for (const tx of valid) {
    await processTransaction(tx, terminalId, log);
  }

  log.info({ processedCount: valid.length }, "ingestion.batch_complete");
}

// Never throws: one bad tap must not stop the rest of the batch.
async function processTransaction(
  tx: ParsedTransaction,
  terminalId: string,
  log: LoggerInstance
): Promise<void> {
  const cardUid = tx.card_uid || tx.student_uid;
  const txLog = log.child({ cardUid, location: tx.location });

  try {
    // The topic is the only identity the broker vouches for. Ignore spoofed device ids.
    if (tx.terminal_id.toUpperCase() !== terminalId.toUpperCase()) {
      txLog.warn(
        { claimedTerminalId: tx.terminal_id },
        "ingestion.terminal_id_mismatch_rejected"
      );
      return;
    }

    // Every terminal runs v1.1.0L, so a line without a location letter is not a valid tap.
    if (!tx.location) {
      txLog.warn("ingestion.legacy_format_rejected");
      await sendAck(terminalId, `ACK:FAIL,${cardUid},MALFORMED_PAYLOAD`, txLog);
      return;
    }

    let result: SettleRideResult;
    try {
      result = await settleRideTransaction({
        terminalId,
        cardUid,
        rawAmount: tx.raw_amount as number,
        timestamp: tx.timestamp as number,
        driverUid: tx.driver_uid || "",
        location: tx.location,
      });
    } catch (err) {
      // The ledger transaction rolled back. No ack: the terminal keeps the tap and resends it.
      txLog.error({ err: errText(err) }, "ingestion.settlement_error_no_ack");
      return;
    }

    if (result.status !== "SUCCESS" && result.status !== "DUPLICATE") {
      txLog.warn({ status: result.status }, "ingestion.settlement_rejected");
    }

    const ack = ackFor(result, cardUid);
    if (ack) {
      await sendAck(terminalId, ack, txLog);
    } else {
      txLog.error({ status: result.status }, "ingestion.unhandled_settlement_status");
    }

    // Tell the fleet when a settled ride pushed the student under the threshold.
    if (
      result.status === "SUCCESS" &&
      (result.belowThreshold ||
        (result.newBalance !== null && isBelowThreshold(result.newBalance)))
    ) {
      broadcastDeltaToFleet(buildDeltaCommand("ADD", "BL", cardUid))
        .then(() => txLog.info("ingestion.blacklist_broadcast_sent"))
        .catch((err) =>
          txLog.error(
            { err: errText(err) },
            "ingestion.post_deduction_blacklist_broadcast_failed"
          )
        );
    }
  } catch (err) {
    txLog.error({ err: errText(err) }, "ingestion.transaction_processing_error");
  }
}

export { ingestTransactionBatch };