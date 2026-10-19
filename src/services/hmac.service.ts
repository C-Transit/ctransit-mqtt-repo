// src/services/hmac.service.ts
import crypto from "node:crypto";
import prisma from "../lib/prisma.js";
import logger from "../config/logger.js";

export interface HmacVerifyResult {
  payload: string;
  terminalId: string;
}

const SIGNATURE_PATTERN = /^[0-9a-f]{64}$/i;

async function getTerminalSecret(terminalId: string): Promise<string | null> {
  const terminal = await prisma.terminal.findUnique({
    where: { terminal_id: terminalId },
    select: { secret_key: true },
  });
  return terminal ? terminal.secret_key : null;
}

function computeHmac(
  secretKey: string,
  terminalId: string,
  payload: string
): string {
  return crypto
    .createHmac("sha256", secretKey)
    .update(`${terminalId}:${payload}`)
    .digest("hex");
}

// Verifies "HMAC:<64 hex>|<payload>" and returns the payload, or null if anything is wrong.
// Never logs the message or payload.
async function verifyAndUnwrap(
  terminalId: string,
  rawMessage: string
): Promise<string | null> {
  const log = logger.child({ terminalId });

  const pipeIndex = rawMessage.indexOf("|");
  if (pipeIndex === -1 || !rawMessage.startsWith("HMAC:")) {
    log.warn("hmac.missing_wrapper — rejecting message");
    return null;
  }

  const receivedSig = rawMessage.slice(5, pipeIndex);
  const payload = rawMessage.slice(pipeIndex + 1);

  if (!receivedSig || !payload) {
    log.warn("hmac.malformed_wrapper — rejecting message");
    return null;
  }

  // Strict hex check: a 64-character non-hex string would decode to a shorter buffer
  // and make timingSafeEqual throw.
  if (!SIGNATURE_PATTERN.test(receivedSig)) {
    log.warn("hmac.invalid_signature_format — rejecting message");
    return null;
  }

  const secretKey = await getTerminalSecret(terminalId);
  if (!secretKey) {
    log.warn("hmac.terminal_not_found — rejecting message");
    return null;
  }
  if (secretKey === "UNPROVISIONED") {
    log.warn("hmac.terminal_unprovisioned — rejecting message");
    return null;
  }

  const expectedSig = computeHmac(secretKey, terminalId, payload);
  const signaturesMatch = crypto.timingSafeEqual(
    Buffer.from(receivedSig, "hex"),
    Buffer.from(expectedSig, "hex")
  );

  if (!signaturesMatch) {
    log.warn("hmac.signature_mismatch — rejecting message");
    return null;
  }

  return payload;
}

export { verifyAndUnwrap, computeHmac };