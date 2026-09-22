import crypto from "crypto";
import { prisma } from "./ledger.service.js";
import logger from "../config/logger.js";

export interface HmacVerifyResult {
  payload: string;
  terminalId: string;
}

// Retrieves the secret key for a given terminal from the database.
async function getTerminalSecret(terminalId: string): Promise<string | null> {
  const terminal = await prisma.terminal.findUnique({
    where: { terminal_id: terminalId },
    select: { secret_key: true },
  });

  if (!terminal) return null;
  return terminal.secret_key;
}

// Computes the HMAC signature for a given payload using the terminal's secret key.
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

// Verifies the HMAC signature of an incoming message and unwraps the payload if valid.
async function verifyAndUnwrap(
  terminalId: string,
  rawMessage: string
): Promise<string | null> {
  const log = logger.child({ terminalId });

  const pipeIndex = rawMessage.indexOf("|");
  if (pipeIndex === -1 || !rawMessage.startsWith("HMAC:")) {
    log.warn(
      { rawMessage: rawMessage.substring(0, 80) },
      "hmac.missing_wrapper — rejecting message"
    );
    return null;
  }

  const receivedSig = rawMessage.slice(5, pipeIndex);
  const payload = rawMessage.slice(pipeIndex + 1);

  if (!receivedSig || !payload) {
    log.warn("hmac.malformed_wrapper — rejecting message");
    return null;
  }

  if (receivedSig.length !== 64) {
    log.warn(
      { sigLength: receivedSig.length },
      "hmac.invalid_signature_length — rejecting"
    );
    return null;
  }

  // Retrieve the terminal's secret key from the database
  const secretKey = await getTerminalSecret(terminalId);

  if (!secretKey) {
    log.warn("hmac.terminal_not_found — rejecting message");
    return null;
  }

  if (secretKey === "UNPROVISIONED") {
    log.warn("hmac.terminal_unprovisioned — rejecting non-status message");
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

  log.debug({ payload: payload.substring(0, 80) }, "hmac.verified");
  return payload;
}

// No-op now — kept so admin.controller.ts import doesn't break
// if it's still referenced anywhere. Safe to remove later.
async function invalidateTerminalSecretCache(): Promise<void> {
}

export { verifyAndUnwrap, invalidateTerminalSecretCache, computeHmac };
