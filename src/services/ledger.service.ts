// src/services/ledger.service.ts
"use strict";

import crypto from "node:crypto";
import prisma from "../lib/prisma.js";
import type { Prisma } from "@prisma/client";
import logger from "../config/logger.js";
import env from "../config/env.js";

type DbClient = typeof prisma | Prisma.TransactionClient;

const PROTOCOL_VERSION = "v1.1.0L";
const VALID_LOCATIONS = new Set(["A", "B", "C"]);
const DRIVER_SHARE_PERCENT = 96;

/** Must match the `SystemWallet.id` default in prisma/schema.prisma. */
const SYSTEM_WALLET_ID = "CTRANSIT_SYSTEM";
const SYSTEM_WALLET_NAME = "C-Transit Platform";

const toNumber = (value: { toString(): string }): number =>
  parseFloat(value.toString());

export interface SettleRideParams {
  terminalId: string; // taken from the MQTT topic, never from the payload
  cardUid: string; // student NFC card UID
  rawAmount: number; // exactly as the terminal sent it; used only for the tap fingerprint
  timestamp: number; // epoch seconds, from the terminal
  driverUid: string; // driver card UID presented on the terminal
  location: string; // A | B | C
}

export type SettlementStatus =
  | "SUCCESS"
  | "DUPLICATE"
  | "INSUFFICIENT_BALANCE"
  | "NOT_WHITELISTED"
  | "BLACKLISTED"
  | "CARD_NOT_REGISTERED"
  | "DRIVER_MISMATCH"
  | "INVALID_REQUEST";

export interface SettleRideResult {
  status: SettlementStatus;
  newBalance: number | null;
  fare: number;
  driverAmount?: number;
  cTransitAmount?: number;
  transactionId?: string;
  belowThreshold?: boolean;
}

function isBelowThreshold(balance: number): boolean {
  return balance < env.ledger.baseFare;
}

// Same inputs, same order as before, so ids of already-settled taps do not change.
function computeFingerprint(p: {
  terminalId: string;
  cardUid: string;
  rawAmount: number;
  timestamp: number;
  driverUid: string;
  location: string;
}): string {
  const input = `${PROTOCOL_VERSION}${p.terminalId}${p.cardUid}${p.rawAmount}${p.timestamp}${p.driverUid}${p.location}`;
  return crypto.createHash("sha256").update(input).digest("hex");
}

function isUniqueViolation(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    (err as { code?: string }).code === "P2002"
  );
}

// If this tap was already settled, answer with the balance of the student who was charged.
async function findDuplicate(
  db: DbClient,
  txId: string
): Promise<SettleRideResult | null> {
  const existing = await db.transaction.findUnique({
    where: { transaction_id: txId },
    select: { student_uid: true, amount: true },
  });
  if (!existing) return null;

  const wallet = await db.wallet.findUnique({
    where: { student_uid: existing.student_uid },
    select: { balance: true },
  });
  return {
    status: "DUPLICATE",
    newBalance: wallet ? toNumber(wallet.balance) : 0,
    fare: toNumber(existing.amount),
    transactionId: txId,
  };
}

async function settleRideTransaction(
  params: SettleRideParams
): Promise<SettleRideResult> {
  const { terminalId, cardUid, rawAmount, timestamp, driverUid, location } =
    params;

  // Fail closed on anything the parser should already have rejected.
  if (
    !cardUid ||
    !driverUid ||
    !VALID_LOCATIONS.has(location) ||
    !Number.isFinite(rawAmount) ||
    rawAmount === 0 ||
    !Number.isFinite(timestamp) ||
    timestamp <= 0
  ) {
    logger.warn({ terminalId, location }, "ledger.invalid_settlement_request");
    return { status: "INVALID_REQUEST", newBalance: null, fare: 0 };
  }

  const txId = computeFingerprint({
    terminalId,
    cardUid,
    rawAmount,
    timestamp,
    driverUid,
    location,
  });
  const log = logger.child({ transactionId: txId, terminalId, location });

  const execute = async (db: Prisma.TransactionClient): Promise<SettleRideResult> => {
    // 1. Idempotency: a resent tap gets the same answer and no second charge.
    const duplicate = await findDuplicate(db, txId);
    if (duplicate) {
      log.info("ledger.settle_ride_duplicate_detected");
      return duplicate;
    }

    // 2. The server owns the fare. The terminal's amount is only checked, never trusted.
    const fareConfig = await db.fareConfig.findUnique({
      where: { code: location },
      select: { amount: true },
    });
    const fare = fareConfig ? toNumber(fareConfig.amount) : 0;
    if (!(fare > 0)) {
      // Server misconfiguration: throw so no ack is sent and the terminal keeps the tap.
      throw new Error(`FARE_NOT_CONFIGURED:${location}`);
    }
    if (Math.abs(rawAmount) !== fare) {
      log.warn(
        { claimedAmount: Math.abs(rawAmount), fare },
        "ledger.fare_mismatch — charging the server fare"
      );
    }

    // 3. Card -> student. No fallback: an unmapped card is not a registered card.
    const mapping = await db.cardMapping.findUnique({
      where: { card_uid: cardUid },
      select: { student_uid: true },
    });
    if (!mapping) {
      log.warn("ledger.card_not_registered");
      return { status: "CARD_NOT_REGISTERED", newBalance: null, fare, transactionId: txId };
    }
    const matricNumber = mapping.student_uid;

    // 4. Blacklist
    const blacklisted = await db.blacklist.findUnique({
      where: { student_uid: matricNumber },
      select: { student_uid: true },
    });
    if (blacklisted) {
      log.warn({ matricNumber }, "ledger.student_is_blacklisted");
      return { status: "BLACKLISTED", newBalance: null, fare, transactionId: txId };
    }

    // 5. Driver must be a registered driver card belonging to an actual DRIVER account.
    const driverCredential = await db.driverCardCredential.findUnique({
      where: { card_uid: driverUid },
      select: { driver_uid: true },
    });
    const driverUser = driverCredential
      ? await db.user.findFirst({
          where: { matricNumber: driverCredential.driver_uid, role: "DRIVER" },
          select: { id: true },
        })
      : null;
    if (!driverCredential || !driverUser) {
      log.warn("ledger.driver_authorization_failed");
      return { status: "DRIVER_MISMATCH", newBalance: null, fare, transactionId: txId };
    }
    const authorizedDriverUid = driverCredential.driver_uid;

    // 6. Guarded debit: one statement checks the balance and takes the money, so two
    //    simultaneous taps can never both pass the check.
    const debit = await db.wallet.updateMany({
      where: { student_uid: matricNumber, is_linked: true, balance: { gte: fare } },
      data: { balance: { decrement: fare } },
    });
    if (debit.count === 0) {
      const wallet = await db.wallet.findUnique({
        where: { student_uid: matricNumber },
        select: { balance: true, is_linked: true },
      });
      if (!wallet || !wallet.is_linked) {
        log.warn({ matricNumber }, "ledger.student_wallet_not_found_or_unlinked");
        return { status: "NOT_WHITELISTED", newBalance: null, fare, transactionId: txId };
      }
      log.warn({ matricNumber, fare }, "ledger.student_insufficient_balance");
      return {
        status: "INSUFFICIENT_BALANCE",
        newBalance: toNumber(wallet.balance),
        fare,
        transactionId: txId,
      };
    }

    const updatedStudent = await db.wallet.findUniqueOrThrow({
      where: { student_uid: matricNumber },
      select: { balance: true },
    });
    const newStudentBalance = toNumber(updatedStudent.balance);

    // 7. Split (96 / 4)
    const driverCredit = Math.round(fare * DRIVER_SHARE_PERCENT) / 100;
    const cTransitCredit = Math.round((fare - driverCredit) * 100) / 100;

    // 8. Ledger row. The unique transaction_id means a concurrent duplicate fails here and
    //    the whole transaction, including the debit above, rolls back.
    await db.transaction.create({
      data: {
        transaction_id: txId,
        type: "RIDE",
        terminal_id: terminalId,
        student_uid: matricNumber,
        amount: fare,
        fare,
        driver_share: driverCredit,
        ctransit_share: cTransitCredit,
        driver_uid: authorizedDriverUid,
        location,
        card_uid: cardUid,
        tapped_at: new Date(timestamp * 1000),
        synced_at: new Date(timestamp * 1000),
      },
    });

    // 9. Credits: driver +96%, platform +4%. Any failure rolls everything back.
    await db.driverWallet.upsert({
      where: { driver_uid: authorizedDriverUid },
      update: {
        balance: { increment: driverCredit },
        total_earnings: { increment: driverCredit },
      },
      create: {
        driver_uid: authorizedDriverUid,
        balance: driverCredit,
        total_earnings: driverCredit,
      },
    });

    await db.systemWallet.upsert({
      where: { id: SYSTEM_WALLET_ID },
      update: {
        balance: { increment: cTransitCredit },
        total_revenue: { increment: cTransitCredit },
      },
      create: {
        id: SYSTEM_WALLET_ID,
        name: SYSTEM_WALLET_NAME,
        balance: cTransitCredit,
        total_revenue: cTransitCredit,
      },
    });

    // 10. Low-balance auto-blacklist
    const belowThreshold = isBelowThreshold(newStudentBalance);
    if (belowThreshold) {
      await db.blacklist.upsert({
        where: { student_uid: matricNumber },
        update: { blacklistedAt: new Date() },
        create: { student_uid: matricNumber, reason: "LOW_BALANCE" },
      });
    }

    log.info(
      { newBalance: newStudentBalance, fare, driverCredit, cTransitCredit, belowThreshold },
      "ledger.ride_settled_96_4_atomic"
    );

    return {
      status: "SUCCESS",
      newBalance: newStudentBalance,
      fare,
      driverAmount: driverCredit,
      cTransitAmount: cTransitCredit,
      transactionId: txId,
      belowThreshold,
    };
  };

  try {
    return await prisma.$transaction(execute, { timeout: 15000 });
  } catch (err: unknown) {
    // A concurrent identical tap lost the race on transaction_id: answer as a duplicate.
    if (isUniqueViolation(err)) {
      const duplicate = await findDuplicate(prisma, txId);
      if (duplicate) {
        log.info("ledger.settle_ride_concurrent_duplicate_resolved");
        return duplicate;
      }
    }
    throw err;
  }
}

export { settleRideTransaction, isBelowThreshold };