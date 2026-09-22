"use strict";

import { activateWallet, prisma } from "./ledger.service.js";
import { buildDeltaCommand, type PendingLinkData } from "../utils/parser.js";
import { routeDeltaToTerminal } from "./sync.service.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";
import { getRedisClient, cacheKeys } from "../config/redis.js";
import logger from "../config/logger.js";

export interface ConfirmRegistrationResult {
  success: boolean;
  message: string;
  matricNumber?: string;
  cardUid?: string;
}

// Handle Current Card linking
async function handlePendingLink(
  terminalId: string,
  linkData: PendingLinkData
): Promise<void> {
  const activeTerminalId = linkData.device_id || terminalId;
  const log = logger.child({
    terminalId: activeTerminalId,
    cardUid: linkData.uid,
    otp: linkData.otp,
    agentUid: linkData.agent_uid,
  });

  const expiresAt = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes

  // If same card taps again, refresh the OTP record
  await prisma.registrationOtp.upsert({
    where: { otp: linkData.otp },
    update: {
      card_uid: linkData.uid,
      terminal_id: activeTerminalId,
      expires_at: expiresAt,
      used: false,
    },
    create: {
      otp: linkData.otp,
      card_uid: linkData.uid,
      terminal_id: activeTerminalId,
      expires_at: expiresAt,
    },
  });

  log.info({ expiresAt, agentUid: linkData.agent_uid }, "registration.otp_saved_to_db");

  try {
    await publishToTerminal(activeTerminalId, `REG:OTP,${linkData.otp}`);
    log.info("registration.otp_sent_to_terminal_screen");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "registration.failed_to_send_otp_to_terminal");
  }
}

// confirmRegistration
async function confirmRegistration(
  otp: string,
  userId: string
): Promise<ConfirmRegistrationResult> {
  const log = logger.child({ userId });

  // Look up OTP in DB
  const otpRecord = await prisma.registrationOtp.findUnique({
    where: { otp },
  });

  if (!otpRecord || otpRecord.used || otpRecord.expires_at < new Date()) {
    log.warn("registration.otp_not_found_or_expired");
    return {
      success: false,
      message: "OTP has expired or is invalid. Please tap your card again.",
    };
  }

  const { card_uid: cardUid, terminal_id: originTerminalId } = otpRecord;

  const user = await prisma.user.findUnique({
    where: { id: userId },
    select: { id: true, matricNumber: true },
  });

  if (!user) {
    log.warn({ userId }, "registration.student_not_found");
    return {
      success: false,
      message: "Student not found. Please check your matric number.",
    };
  }

  log.info(
    { cardUid, originTerminalId, matricNumber: user.matricNumber },
    "registration.otp_confirmed"
  );

  try {
    await activateWallet(user.matricNumber);
    log.info(
      { matricNumber: user.matricNumber },
      "registration.wallet_activated"
    );
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "registration.wallet_activation_failed");
    return {
      success: false,
      message: "Database error during wallet activation.",
    };
  }

  await prisma.cardMapping.upsert({
    where: { card_uid: cardUid },
    update: { student_uid: user.matricNumber },
    create: {
      card_uid: cardUid,
      student_uid: user.matricNumber,
    },
  });

  log.info(
    { cardUid, matricNumber: user.matricNumber },
    "registration.card_uid_mapped_to_student"
  );

  // Invalidate card map cache — new mapping must be picked up immediately
  const redis = getRedisClient();
  await redis.del(cacheKeys.cardMap(cardUid));
  log.debug({ cardUid }, "registration.card_map_cache_invalidated");

  // Mark OTP as used — cannot be reused
  await prisma.registrationOtp.update({
    where: { otp },
    data: { used: true },
  });
  log.debug("registration.otp_consumed");

  const addWlCmd = buildDeltaCommand("ADD", "WL", cardUid);

  try {
    await routeDeltaToTerminal(originTerminalId, addWlCmd);
    log.info(
      { originTerminalId, addWlCmd },
      "registration.origin_terminal_synced"
    );
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "registration.origin_sync_failed");
  }

  try {
    const allTerminals = await prisma.terminal.findMany({
      where: { terminal_id: { not: originTerminalId } },
      select: { terminal_id: true },
    });
    await Promise.allSettled(
      allTerminals.map((t: { terminal_id: string }) =>
        routeDeltaToTerminal(t.terminal_id, addWlCmd)
      )
    );
    log.info(
      { terminalCount: allTerminals.length },
      "registration.fleet_sync_queued"
    );
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "registration.fleet_sync_error");
  }

  return {
    success: true,
    message: "Card successfully linked and activated.",
    matricNumber: user.matricNumber,
    cardUid,
  };
}

export { handlePendingLink, confirmRegistration };
