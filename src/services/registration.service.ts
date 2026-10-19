// src/services/registration.service.ts
"use strict";

import prisma from "../lib/prisma.js";
import type { PendingLinkData } from "../utils/parser.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";
import logger from "../config/logger.js";

const OTP_TTL_MS = 5 * 60 * 1000; // 5 minutes

// A terminal reports "this card wants to be linked, here is its 6-digit code".
// The backend later matches the code to a student and links the card.
async function handlePendingLink(
  terminalId: string,
  linkData: PendingLinkData
): Promise<void> {
  const log = logger.child({ terminalId, cardUid: linkData.uid });

  const now = new Date();
  const expiresAt = new Date(now.getTime() + OTP_TTL_MS);

  // A live code that already belongs to a DIFFERENT card must never be overwritten:
  // otherwise a student could end up linking someone else's card.
  const existing = await prisma.registrationOtp.findUnique({
    where: { otp: linkData.otp },
    select: { card_uid: true, used: true, expires_at: true },
  });

  if (
    existing &&
    !existing.used &&
    existing.expires_at > now &&
    existing.card_uid !== linkData.uid
  ) {
    log.warn("registration.otp_collision_rejected");
    return;
  }

  // Same card tapping again refreshes its own record.
  await prisma.registrationOtp.upsert({
    where: { otp: linkData.otp },
    update: {
      card_uid: linkData.uid,
      terminal_id: terminalId,
      expires_at: expiresAt,
      used: false,
    },
    create: {
      otp: linkData.otp,
      card_uid: linkData.uid,
      terminal_id: terminalId,
      expires_at: expiresAt,
    },
  });

  log.info({ expiresAt }, "registration.otp_saved_to_db");

  try {
    await publishToTerminal(terminalId, `REG:OTP,${linkData.otp}`);
    log.info("registration.otp_sent_to_terminal_screen");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "registration.failed_to_send_otp_to_terminal");
  }
}

export { handlePendingLink };