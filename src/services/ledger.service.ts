"use strict";

import crypto from "node:crypto";
import prisma from "../lib/prisma.js";
import type { Prisma } from "@prisma/client";
import logger from "../config/logger.js";
import env from "../config/env.js";

// Support both standard PrismaClient and Transactional Client
type DbClient = typeof prisma | Prisma.TransactionClient;

interface DeductionResult {
  newBalance: number | null;
  walletFound: boolean;
}

interface CreditResult {
  previousBalance: number;
  newBalance: number;
}

async function deductFare(
  studentUid: string,
  amount: number,
  transactionId: string,
  dbClient: DbClient = prisma
): Promise<DeductionResult> {
  const childLogger = logger.child({ transactionId, studentUid, amount });
  const wallet = await dbClient.wallet.findUnique({
    where: { student_uid: studentUid },
    select: { balance: true },
  });

  if (!wallet) {
    childLogger.warn("ledger.wallet_not_found — skipping deduction");
    return { newBalance: null, walletFound: false };
  }

  const updatedWallet = await dbClient.wallet.update({
    where: { student_uid: studentUid },
    data: { balance: { decrement: amount } },
    select: { balance: true },
  });

  const newBalance = parseFloat(updatedWallet.balance.toString());
  childLogger.info(
    { previousBalance: parseFloat(wallet.balance.toString()), newBalance },
    "ledger.fare_deducted"
  );
  return { newBalance, walletFound: true };
}

function isBelowThreshold(balance: number): boolean {
  return balance < env.ledger.baseFare;
}

function hasCrossedAboveThreshold(
  previousBalance: number,
  newBalance: number
): boolean {
  return (
    previousBalance < env.ledger.baseFare && newBalance >= env.ledger.baseFare
  );
}

async function activateWallet(studentUid: string): Promise<void> {
  await prisma.wallet.upsert({
    where: { student_uid: studentUid },
    update: { is_linked: true },
    create: { student_uid: studentUid, balance: 0, is_linked: true },
  });
  logger.info({ studentUid }, "ledger.wallet_activated");
}

async function creditWallet(
  studentUid: string,
  amount: number,
  dbClient: DbClient = prisma
): Promise<CreditResult | null> {
  const wallet = await dbClient.wallet.findUnique({
    where: { student_uid: studentUid },
    select: { balance: true },
  });

  if (!wallet) {
    logger.warn({ studentUid }, "ledger.credit_wallet_not_found");
    return null;
  }

  const updatedWallet = await dbClient.wallet.update({
    where: { student_uid: studentUid },
    data: { balance: { increment: amount } },
    select: { balance: true },
  });

  const previousBalance = parseFloat(wallet.balance.toString());
  const newBalance = parseFloat(updatedWallet.balance.toString());
  logger.info(
    { studentUid, previousBalance, newBalance, amount },
    "ledger.wallet_credited"
  );
  return { previousBalance, newBalance };
}

export interface SettleRideParams {
  transactionId?: string;
  terminalId: string;
  studentUid: string;
  cardUid?: string;
  rawAmount?: number;
  amount: number;
  timestamp?: number;
  driverUid: string | null;
  location?: string | null;
  protocolVersion?: string;
  syncedAt?: Date;
}

export type SettlementStatus =
  | "SUCCESS"
  | "DUPLICATE"
  | "INSUFFICIENT_BALANCE"
  | "NOT_WHITELISTED"
  | "BLACKLISTED"
  | "CARD_NOT_REGISTERED"
  | "DRIVER_MISMATCH"
  | "NOT_FOUND";

export interface SettleRideResult {
  status: SettlementStatus;
  newBalance: number | null;
  fare: number;
  driverAmount?: number;
  cTransitAmount?: number;
  transactionId?: string;
  belowThreshold?: boolean;
}

// Fingerprinting backend security link
export function computeDeterministicFingerprint(params: {
  protocolVersion?: string;
  terminalId: string;
  cardUid: string;
  rawAmount: number;
  timestamp: number;
  driverUid: string | null;
  location: string;
}): string {
  const proto = params.protocolVersion || "v1.1.0L";
  const drv = params.driverUid || "";
  const input = `${proto}${params.terminalId}${params.cardUid}${params.rawAmount}${params.timestamp}${drv}${params.location}`;
  return crypto.createHash("sha256").update(input).digest("hex");
}

/**
 * Canonical id of the C-Transit system wallet row.
 * Must match the `SystemWallet.id` default in prisma/schema.prisma.
 */
const SYSTEM_WALLET_ID = "CTRANSIT_SYSTEM";
const SYSTEM_WALLET_NAME = "C-Transit Platform";

async function settleRideTransaction(
  params: SettleRideParams,
  dbClient: DbClient = prisma
): Promise<SettleRideResult> {
  const {
    transactionId,
    terminalId,
    studentUid,
    cardUid,
    rawAmount,
    amount,
    timestamp,
    driverUid,
    location,
    protocolVersion,
    syncedAt,
  } = params;

  const effectiveCardUid = cardUid || studentUid;
  const isV110L = protocolVersion === "v1.1.0L" || Boolean(location);

  // Determine deterministic transactionId
  let txId: string;
  if (location) {
    const proto = protocolVersion || "v1.1.0L";
    const rawAmt = rawAmount !== undefined ? rawAmount : -Math.abs(amount);
    const ts =
      timestamp !== undefined
        ? timestamp
        : Math.floor((syncedAt?.getTime() ?? Date.now()) / 1000);
    txId = computeDeterministicFingerprint({
      protocolVersion: proto,
      terminalId,
      cardUid: effectiveCardUid,
      rawAmount: rawAmt,
      timestamp: ts,
      driverUid: driverUid || null,
      location,
    });
  } else {
    // Legacy fallback format
    const ts =
      timestamp !== undefined
        ? timestamp
        : Math.floor((syncedAt?.getTime() ?? Date.now()) / 1000);
    txId = transactionId || `${terminalId}-${ts}-${effectiveCardUid}`;
  }

  const childLogger = logger.child({
    transactionId: txId,
    cardUid: effectiveCardUid,
    amount,
    driverUid,
    terminalId,
    location,
  });

  const execute = async (tx_ctx: Prisma.TransactionClient) => {
    // 1. Idempotency Check
    const existingTx = await tx_ctx.transaction.findUnique({
      where: { transaction_id: txId },
    });

    if (existingTx) {
      childLogger.info(
        { transactionId: txId },
        "ledger.settle_ride_duplicate_detected"
      );

      // Resolve card UID to check current balance for idempotent ACK
      const mapping = await tx_ctx.cardMapping.findUnique({
        where: { card_uid: effectiveCardUid },
        select: { student_uid: true },
      });
      const resolvedMatric = mapping ? mapping.student_uid : effectiveCardUid;
      const currentWallet = await tx_ctx.wallet.findUnique({
        where: { student_uid: resolvedMatric },
        select: { balance: true },
      });
      const currentBalance = currentWallet
        ? parseFloat(currentWallet.balance.toString())
        : 0;
      return {
        status: "DUPLICATE" as const,
        newBalance: currentBalance,
        fare: amount,
        transactionId: txId,
      };
    }

    // 2. Card Resolution (Backend owns CardMapping)
    let matricNumber = effectiveCardUid;
    const mapping = await tx_ctx.cardMapping.findUnique({
      where: { card_uid: effectiveCardUid },
      select: { student_uid: true },
    });

    if (mapping) {
      matricNumber = mapping.student_uid;
    } else {
      // Check if effectiveCardUid directly corresponds to an existing wallet/user (for backwards compatibility)
      const directWallet = await tx_ctx.wallet.findUnique({
        where: { student_uid: effectiveCardUid },
        select: { student_uid: true },
      });
      if (!directWallet) {
        childLogger.warn(
          { cardUid: effectiveCardUid },
          "ledger.card_not_registered"
        );
        return {
          status: "CARD_NOT_REGISTERED" as const,
          newBalance: null,
          fare: amount,
          transactionId: txId,
        };
      }
    }

    // 3. Blacklist Check (Backend owns domain validation)
    const blacklisted = await tx_ctx.blacklist.findUnique({
      where: { student_uid: matricNumber },
      select: { student_uid: true },
    });
    if (blacklisted) {
      childLogger.warn(
        { matricNumber, cardUid: effectiveCardUid },
        "ledger.student_is_blacklisted"
      );
      return {
        status: "BLACKLISTED" as const,
        newBalance: null,
        fare: amount,
        transactionId: txId,
      };
    }

    // 4. Verify Student Wallet & Balance
    const studentWallet = await tx_ctx.wallet.findUnique({
      where: { student_uid: matricNumber },
      select: { balance: true, is_linked: true },
    });

    if (!studentWallet || !studentWallet.is_linked) {
      childLogger.warn(
        { matricNumber },
        "ledger.student_wallet_not_found_or_unlinked"
      );
      return {
        status: "NOT_WHITELISTED" as const,
        newBalance: null,
        fare: amount,
        transactionId: txId,
      };
    }

    const currentStudentBalance = parseFloat(studentWallet.balance.toString());
    if (currentStudentBalance < amount) {
      childLogger.warn(
        { balance: currentStudentBalance, fare: amount, matricNumber },
        "ledger.student_insufficient_balance"
      );
      return {
        status: "INSUFFICIENT_BALANCE" as const,
        newBalance: currentStudentBalance,
        fare: amount,
        transactionId: txId,
      };
    }

    // 5. Driver Authorization (Direct DB verification)
    let authorizedDriverUid: string | null;

    if (isV110L) {
      if (!driverUid) {
        childLogger.warn(
          { terminalId, reason: "DRIVER_MISMATCH" },
          "ledger.driver_authorization_failed — no driverUid provided in payload"
        );
        return {
          status: "DRIVER_MISMATCH" as const,
          newBalance: currentStudentBalance,
          fare: amount,
          transactionId: txId,
        };
      }

      // Verify the payload card UID exists in DriverCardCredential
      const driverCredential = await tx_ctx.driverCardCredential.findUnique({
        where: { card_uid: driverUid },
        select: { driver_uid: true },
      });

      if (!driverCredential) {
        childLogger.warn(
          {
            payloadDriverUid: driverUid,
            terminalId,
            reason: "DRIVER_MISMATCH",
          },
          "ledger.driver_authorization_failed — unregistered driver card"
        );
        return {
          status: "DRIVER_MISMATCH" as const,
          newBalance: currentStudentBalance,
          fare: amount,
          transactionId: txId,
        };
      }

      // Verify the driver user profile is active and has the correct DRIVER role
      const driverUser = await tx_ctx.user.findFirst({
        where: { matricNumber: driverCredential.driver_uid, role: "DRIVER" },
        select: { id: true },
      });

      if (!driverUser) {
        childLogger.warn(
          {
            payloadDriverUid: driverUid,
            resolvedMatric: driverCredential.driver_uid,
            terminalId,
            reason: "DRIVER_MISMATCH",
          },
          "ledger.driver_authorization_failed — active driver not registered in database or not a DRIVER"
        );
        return {
          status: "DRIVER_MISMATCH" as const,
          newBalance: currentStudentBalance,
          fare: amount,
          transactionId: txId,
        };
      }

      authorizedDriverUid = driverCredential.driver_uid;
    } else {
      // Legacy behavior: use the terminal's active driver state
      const terminal = await tx_ctx.terminal.findUnique({
        where: { terminal_id: terminalId },
        select: { active_driver_uid: true },
      });
      authorizedDriverUid = terminal?.active_driver_uid || null;

      if (driverUid && driverUid !== authorizedDriverUid) {
        childLogger.warn(
          {
            payloadDriverUid: driverUid,
            assignedDriverUid: authorizedDriverUid,
          },
          "ledger.driver_authorization — payload driver differs from terminal active driver, using authorized driver"
        );
      }
    }

    // 6. Compute 4% fee deduction
    const driverCredit = Math.round(amount * 96) / 100;
    const cTransitCredit = Math.round((amount - driverCredit) * 100) / 100;

    // 7. Create Transaction Record (with full split persisted for audit)
    await tx_ctx.transaction.create({
      data: {
        transaction_id: txId,
        type: "RIDE",
        terminal_id: terminalId,
        student_uid: matricNumber,
        amount: amount,
        fare: amount,
        driver_share: driverCredit,
        ctransit_share: cTransitCredit,
        driver_uid: authorizedDriverUid,
        location: location || null,
        card_uid: effectiveCardUid,
        tapped_at: timestamp !== undefined ? new Date(timestamp * 1000) : null,
        synced_at:
          syncedAt ?? (timestamp ? new Date(timestamp * 1000) : new Date()),
      },
    });

    // 8. Atomic Balance Updates

    // A) Student: -100%
    const updatedStudentWallet = await tx_ctx.wallet.update({
      where: { student_uid: matricNumber },
      data: { balance: { decrement: amount } },
      select: { balance: true },
    });
    const newStudentBalance = parseFloat(
      updatedStudentWallet.balance.toString()
    );

    // B) Driver: +96%  → uses DriverWallet, NOT Wallet
    if (authorizedDriverUid) {
      await tx_ctx.driverWallet.upsert({
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
    }

    // C) C-Transit Platform: +4%  → uses SystemWallet, NOT Wallet
    // No `.catch()` — if this fails, the whole transaction must roll back.
    await tx_ctx.systemWallet.upsert({
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

    // 9. Low-balance auto-blacklisting in database
    const belowThreshold = isBelowThreshold(newStudentBalance);
    if (belowThreshold) {
      await tx_ctx.blacklist.upsert({
        where: { student_uid: matricNumber },
        update: { blacklistedAt: new Date() },
        create: { student_uid: matricNumber, reason: "LOW_BALANCE" },
      });
    }

    childLogger.info(
      {
        previousBalance: currentStudentBalance,
        newBalance: newStudentBalance,
        fare: amount,
        driverCredit,
        cTransitCredit,
        transactionId: txId,
        belowThreshold,
      },
      "ledger.ride_settled_96_4_atomic"
    );

    return {
      status: "SUCCESS" as const,
      newBalance: newStudentBalance,
      fare: amount,
      driverAmount: driverCredit,
      cTransitAmount: cTransitCredit,
      transactionId: txId,
      belowThreshold,
    };
  };

  try {
    if (
      "$transaction" in dbClient &&
      typeof dbClient.$transaction === "function"
    ) {
      return await (dbClient as typeof prisma).$transaction(execute, {
        timeout: 15000,
      });
    } else {
      return await execute(dbClient as Prisma.TransactionClient);
    }
  } catch (err: unknown) {
    // If concurrent insert failed with unique constraint on transaction_id, resolve as DUPLICATE
    const isUniqueConstraint =
      typeof err === "object" &&
      err !== null &&
      (("code" in err && err.code === "P2002") ||
        ("message" in err &&
          typeof err.message === "string" &&
          (err.message.includes("Unique constraint") ||
            err.message.includes("duplicate key"))));

    if (isUniqueConstraint) {
      childLogger.info(
        { transactionId: txId },
        "ledger.settle_ride_concurrent_duplicate_resolved"
      );
      const existing = await dbClient.transaction.findUnique({
        where: { transaction_id: txId },
      });
      if (existing) {
        const mapping = await dbClient.cardMapping.findUnique({
          where: { card_uid: effectiveCardUid },
          select: { student_uid: true },
        });
        const resolvedMatric = mapping ? mapping.student_uid : effectiveCardUid;
        const currentWallet = await dbClient.wallet.findUnique({
          where: { student_uid: resolvedMatric },
          select: { balance: true },
        });
        const currentBalance = currentWallet
          ? parseFloat(currentWallet.balance.toString())
          : 0;
        return {
          status: "DUPLICATE" as const,
          newBalance: currentBalance,
          fare: amount,
          transactionId: txId,
        };
      }
    }
    throw err;
  }
}

export {
  deductFare,
  settleRideTransaction,
  isBelowThreshold,
  hasCrossedAboveThreshold,
  activateWallet,
  creditWallet,
  prisma,
};
