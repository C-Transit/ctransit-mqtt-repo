// src/services/ingestion.service.ts
"use strict";

import { prisma, isBelowThreshold } from "./ledger.service.js";
import type { Prisma } from "@prisma/client";
import logger from "../config/logger.js";
import {
  parseTransactionBatch,
  buildDeltaCommand,
  type ParsedTransaction,
} from "../utils/parser.js";
import { broadcastDeltaToFleet } from "./sync.service.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";
import { getRedisClient, cacheKeys } from "../config/redis.js";

type LoggerInstance = typeof logger;

const CARD_CACHE_TTL_SECONDS = 604800; // 7 days TTL

// ── Cache Helper: Card Mapping ────────────────────────────────────────────────
// Maps hardware card UID → student matricNumber in Redis with a 7-day TTL.
// Invalidated on card re-link in registration.service.ts.
async function resolveCardUid(cardUid: string): Promise<string | null> {
  const redis = getRedisClient();
  const key = cacheKeys.cardMap(cardUid);

  const cached = await redis.get(key);
  if (cached) return cached as string;

  const mapping = await prisma.cardMapping.findUnique({
    where: { card_uid: cardUid },
    select: { student_uid: true },
  });

  if (!mapping) return null;

  // Cache with 7-day TTL
  await redis.set(key, mapping.student_uid, "EX", CARD_CACHE_TTL_SECONDS);

  return mapping.student_uid;
}

// ── Main Ingestion Logic ──────────────────────────────────────────────────────

async function ingestTransactionBatch(
  terminalId: string,
  rawPayload: string
): Promise<void> {
  const log = logger.child({ terminalId });
  log.info({ payloadLength: rawPayload.length }, "ingestion.batch_received");

  const { valid, invalid } = parseTransactionBatch(rawPayload, terminalId);

  if (invalid.length > 0) {
    log.warn({ invalidRows: invalid }, "ingestion.invalid_rows_skipped");
  }

  if (valid.length === 0) {
    log.warn("ingestion.no_valid_rows");
    return;
  }

  // Optimize: query terminal active driver once per batch instead of per transaction
  const terminal = await prisma.terminal.findUnique({
    where: { terminal_id: terminalId },
    select: { active_driver_uid: true },
  });
  const defaultDriverUid = terminal?.active_driver_uid || null;

  for (const tx of valid) {
    await processTransaction(tx, terminalId, defaultDriverUid, log);
  }

  log.info({ processedCount: valid.length }, "ingestion.batch_complete");
}

async function processTransaction(
  tx: ParsedTransaction,
  terminalId: string,
  defaultDriverUid: string | null,
  log: LoggerInstance
): Promise<void> {
  const txLog = log.child({
    transactionId: tx.transaction_id,
    cardUid: tx.student_uid,
  });

  try {
    // ── STEP 0: Resolve card UID → matricNumber (Redis cached with TTL) ───
    const matricNumber = await resolveCardUid(tx.student_uid);

    if (!matricNumber) {
      txLog.warn("ingestion.card_uid_not_registered");
      await publishToTerminal(
        terminalId,
        `ACK:FAIL,${tx.student_uid},CARD_NOT_REGISTERED`
      );
      return;
    }

    txLog.info({ matricNumber }, "ingestion.card_uid_resolved");

    // ── STEP 1 & 2: Check wallet whitelist and blacklist in parallel ──────
    const [wallet, blacklisted] = await Promise.all([
      prisma.wallet.findUnique({
        where: { student_uid: matricNumber },
        select: { balance: true, is_linked: true },
      }),
      prisma.blacklist.findUnique({
        where: { student_uid: matricNumber },
        select: { student_uid: true },
      }),
    ]);

    const walletData = wallet
      ? {
          balance: parseFloat(wallet.balance.toString()),
          is_linked: wallet.is_linked,
        }
      : null;

    if (!walletData || !walletData.is_linked) {
      txLog.warn({ matricNumber }, "ingestion.student_not_whitelisted");
      await publishToTerminal(
        terminalId,
        `ACK:FAIL,${tx.student_uid},NOT_WHITELISTED`
      );
      return;
    }

    if (blacklisted) {
      txLog.warn({ matricNumber }, "ingestion.student_is_blacklisted");
      await publishToTerminal(
        terminalId,
        `ACK:FAIL,${tx.student_uid},BLACKLISTED`
      );
      return;
    }

    const currentBalance = walletData.balance;
    const driverUid = tx.driver_uid || defaultDriverUid;

    // ── All checks passed — deduct atomically with idempotency ─────────────
    const result = await prisma.$transaction(
      async (tx_ctx: Prisma.TransactionClient) => {
        const existingTx = await tx_ctx.transaction.findUnique({
          where: { transaction_id: tx.transaction_id },
        });

        if (existingTx) {
          txLog.debug("ingestion.duplicate_transaction_skipped");
          return { duplicate: true, newBalance: currentBalance };
        }

        await tx_ctx.transaction.create({
          data: {
            transaction_id: tx.transaction_id,
            type: "RIDE",
            terminal_id: tx.terminal_id,
            student_uid: matricNumber,
            amount: tx.amount,
            driver_uid: driverUid,
            synced_at: tx.synced_at,
          },
        });

        const updatedWallet = await tx_ctx.wallet.update({
          where: { student_uid: matricNumber },
          data: { balance: { decrement: tx.amount } },
          select: { balance: true },
        });

        const newBalance = parseFloat(updatedWallet.balance.toString());

        txLog.info(
          {
            previousBalance: currentBalance,
            newBalance,
            driverUid,
            matricNumber,
          },
          "ingestion.fare_deducted"
        );

        return { duplicate: false, newBalance };
      },
      { timeout: 15000 }
    );

    if (result.duplicate) return;

    const { newBalance } = result;

    // ACK to terminal — no cache to invalidate, DB is source of truth
    await publishToTerminal(
      terminalId,
      `ACK:OK,${tx.student_uid},${newBalance.toFixed(2)}`
    );
    txLog.info({ newBalance, matricNumber }, "ingestion.ack_sent_to_terminal");

    // ── Post-deduction blacklist check ────────────────────────────────────
    if (isBelowThreshold(newBalance)) {
      const blacklistCmd = buildDeltaCommand("ADD", "BL", tx.student_uid);
      txLog.info(
        { newBalance, matricNumber },
        "ingestion.balance_now_below_threshold — blacklisting"
      );

      process.nextTick(async () => {
        try {
          await prisma.blacklist.upsert({
            where: { student_uid: matricNumber },
            update: { blacklistedAt: new Date() },
            create: { student_uid: matricNumber, reason: "LOW_BALANCE" },
          });

          await broadcastDeltaToFleet(blacklistCmd);

          txLog.info({ blacklistCmd }, "ingestion.blacklist_broadcast_sent");
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : "Unknown error";
          txLog.error(
            { err: errMsg },
            "ingestion.post_deduction_blacklist_failed"
          );
        }
      });
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    txLog.error({ err: errMsg }, "ingestion.transaction_processing_error");
    try {
      await publishToTerminal(
        terminalId,
        `ACK:FAIL,${tx.student_uid},SERVER_ERROR`
      );
    } catch (ackErr) {
      const ackErrMsg =
        ackErr instanceof Error ? ackErr.message : "Unknown error";
      txLog.error({ err: ackErrMsg }, "ingestion.ack_send_failed");
    }
  }
}

export { ingestTransactionBatch };
