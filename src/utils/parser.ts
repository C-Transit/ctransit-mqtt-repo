// src/utils/parser.ts
"use strict";

// Strict error handling across all parsers
export type ParseResult<T> =
  | { error: string; data?: undefined }
  | { data: T; error?: undefined };

export const PAYLOAD_TYPE = {
  TRANSACTION_BATCH: "TRANSACTION_BATCH",
  PENDING_LINK: "PENDING_LINK",
  SYNC_REQUEST: "SYNC_REQUEST",
  SYS_FULL_SYNC: "SYS_FULL_SYNC",
  DRIVER_EVENT: "DRIVER_EVENT",
  UNKNOWN: "UNKNOWN",
} as const;

export type PayloadType = (typeof PAYLOAD_TYPE)[keyof typeof PAYLOAD_TYPE];

export interface ParsedTransaction {
  transaction_id: string;
  terminal_id: string;
  student_uid: string;
  card_uid?: string;
  amount: number;
  raw_amount?: number;
  timestamp?: number;
  driver_uid: string | null;
  location?: string | null;
  synced_at: Date;
}

export interface TransactionBatchResult {
  valid: ParsedTransaction[];
  invalid: Array<{ line: string; reason: string }>;
}

export interface DriverEventData {
  action: "LOGIN" | "LOGOUT";
  driverUid: string;
}

export interface PendingLinkData {
  uid: string;
  otp: string;
  agent_uid: string;
  device_id?: string;
}

export interface SyncRequestData {
  device_id: string;
  timestamp: number;
  driver_uid: string;
  location: string;
}

export interface BatchSplitResult {
  deviceId: string;
  lines: string[];
}

// Card UIDs and driver card UIDs are at most 20 characters in the database.
const CARD_UID_PATTERN = /^[A-Za-z0-9_-]{1,20}$/;
const AMOUNT_PATTERN = /^-?\d+(\.\d{1,2})?$/;
// Epoch seconds. 7 to 11 digits so a terminal with an unsynced clock is still accepted, as before.
const TIMESTAMP_PATTERN = /^\d{7,11}$/;

// Splits a message into lines. A leading "DEVICE:" prefix is returned as deviceId.
export function splitBatchPayload(
  raw: string,
  fallbackTerminalId: string
): BatchSplitResult {
  const trimmed = raw.trim();
  let deviceId = fallbackTerminalId;
  let body = trimmed;

  if (
    body.includes(":") &&
    !body.startsWith("PENDING_LINK:") &&
    !body.startsWith("SYNC_REQUEST") &&
    !body.startsWith("SYS:") &&
    !body.startsWith("DRV:")
  ) {
    const colonIdx = body.indexOf(":");
    const possibleDevice = body.substring(0, colonIdx).trim();
    if (possibleDevice && !possibleDevice.includes(",") && !possibleDevice.includes("|")) {
      deviceId = possibleDevice.toUpperCase();
      body = body.substring(colonIdx + 1).trim();
    }
  }

  const lines = body
    .split(/[\n|]/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  // Only the first record may carry a "<TOPIC_TERMINAL_ID>," prefix. Doubles as a
  // second identity check: a different id here is surfaced as deviceId so the
  // caller's existing mismatch guard rejects it.
  if (lines.length > 0) {
    const first = lines[0];
    const commaIdx = first.indexOf(",");
    if (commaIdx > 0) {
      const firstField = first.substring(0, commaIdx).trim();
      const remainder = first.substring(commaIdx + 1).trim();
      if (CARD_UID_PATTERN.test(firstField)) {
        const isTap = remainder.split(",").length === 5;
        const isPending = remainder.toUpperCase().startsWith("PENDING_LINK:");
        if (isTap || isPending) {
          if (firstField.toUpperCase() === fallbackTerminalId.toUpperCase()) {
            lines[0] = remainder;
          } else {
            deviceId = firstField.toUpperCase();
          }
        }
      }
    }
  }

  return { deviceId, lines };
}

export function detectPayloadType(raw: string): PayloadType {
  let trimmed = raw.trim();

  // Strip leading device prefix if present on an individual line
  if (
    trimmed.includes(":") &&
    !trimmed.startsWith("PENDING_LINK:") &&
    !trimmed.startsWith("SYNC_REQUEST") &&
    !trimmed.startsWith("SYS:") &&
    !trimmed.startsWith("DRV:")
  ) {
    const colonIdx = trimmed.indexOf(":");
    const prefix = trimmed.substring(0, colonIdx);
    if (!prefix.includes(",") && !prefix.includes("|")) {
      trimmed = trimmed.substring(colonIdx + 1).trim();
    }
  }

  if (trimmed.startsWith("PENDING_LINK:")) return PAYLOAD_TYPE.PENDING_LINK;
  if (trimmed.startsWith("SYNC_REQUEST")) return PAYLOAD_TYPE.SYNC_REQUEST;
  if (trimmed === "SYS:REQ_FULL_SYNC") return PAYLOAD_TYPE.SYS_FULL_SYNC;
  if (trimmed.startsWith("DRV:")) return PAYLOAD_TYPE.DRIVER_EVENT;

  if (
    trimmed.startsWith("SYS:") ||
    trimmed.startsWith("ACK:") ||
    trimmed.startsWith("ERR:") ||
    trimmed.startsWith("REG:")
  ) {
    return PAYLOAD_TYPE.UNKNOWN;
  }
  return PAYLOAD_TYPE.TRANSACTION_BATCH;
}

export function parseDriverEvent(raw: string): ParseResult<DriverEventData> {
  let body = raw.trim();
  if (body.includes("DRV:")) {
    body = body.substring(body.indexOf("DRV:") + 4).trim();
  }
  const parts = body.split(",").map((p) => p.trim());

  if (parts.length !== 2) {
    return { error: `DRV event expects 2 fields, got ${parts.length}` };
  }

  const [action, driverUid] = parts;

  if (action !== "LOGIN" && action !== "LOGOUT") {
    return { error: "Unknown DRV action. Expected LOGIN or LOGOUT" };
  }

  if (!driverUid || driverUid.length === 0) {
    return { error: "driver_uid is empty" };
  }

  return { data: { action, driverUid } };
}

export function parseTransactionBatch(
  raw: string,
  terminalId: string
): TransactionBatchResult {
  const { deviceId, lines } = splitBatchPayload(raw, terminalId);
  const activeTerminalId = deviceId || terminalId;

  const valid: ParsedTransaction[] = [];
  const invalid: Array<{ line: string; reason: string }> = [];

  for (const line of lines) {
    const parsed = parseSingleTransaction(line, activeTerminalId);
    if (parsed.error) {
      invalid.push({ line, reason: parsed.error });
    } else if (parsed.data) {
      valid.push(parsed.data);
    }
  }

  return { valid, invalid };
}

// v1.1.0L tap line: cardUid,amount,timestamp,driverCardUid,location
// Error messages never contain the line's values.
export function parseSingleTransaction(
  line: string,
  terminalId: string
): ParseResult<ParsedTransaction> {
  const parts = line.trim().split(",").map((p) => p.trim());

  if (parts.length !== 5) {
    return { error: `Expected 5 CSV fields, got ${parts.length}` };
  }

  const [cardUid, amountRaw, timestampRaw, driverUid, locationRaw] = parts;

  if (!CARD_UID_PATTERN.test(cardUid)) return { error: "Invalid card UID" };
  if (!AMOUNT_PATTERN.test(amountRaw)) return { error: "Invalid amount" };

  const rawAmount = parseFloat(amountRaw);
  if (rawAmount === 0) return { error: "Amount is zero" };

  if (!TIMESTAMP_PATTERN.test(timestampRaw)) return { error: "Invalid timestamp" };
  const epochSeconds = parseInt(timestampRaw, 10);

  if (!CARD_UID_PATTERN.test(driverUid)) return { error: "Invalid driver UID" };

  const location = locationRaw.toUpperCase();
  if (location !== "A" && location !== "B" && location !== "C") {
    return { error: "Invalid location code. Expected A, B, or C" };
  }

  return {
    data: {
      transaction_id: `${terminalId}-${epochSeconds}-${cardUid}`,
      terminal_id: terminalId,
      student_uid: cardUid,
      card_uid: cardUid,
      amount: Math.abs(rawAmount),
      raw_amount: rawAmount,
      timestamp: epochSeconds,
      driver_uid: driverUid,
      location,
      synced_at: new Date(epochSeconds * 1000),
    },
  };
}

export function parsePendingLink(
  raw: string,
  terminalId?: string
): ParseResult<PendingLinkData> {
  let deviceId = terminalId;
  let body = raw.trim();

  // Handle "{device_id}:PENDING_LINK:..." or "PENDING_LINK:..."
  if (body.includes("PENDING_LINK:")) {
    const idx = body.indexOf("PENDING_LINK:");
    if (idx > 0) {
      const prefix = body.substring(0, idx).replace(/:$/, "").trim();
      if (prefix) deviceId = prefix.toUpperCase();
    }
    body = body.substring(idx + "PENDING_LINK:".length).trim();
  }

  const parts = body.split(",").map((p) => p.trim());

  if (parts.length !== 3) {
    return {
      error: `PENDING_LINK expects 3 fields after prefix, got ${parts.length}`,
    };
  }

  const [uid, otp, agent_uid] = parts;

  if (!uid || !CARD_UID_PATTERN.test(uid))
    return { error: "Invalid UID in PENDING_LINK" };
  if (!otp || !/^\d{6}$/.test(otp)) return { error: "OTP must be 6 digits" };
  if (!agent_uid || agent_uid.length === 0)
    return { error: "agent_uid is empty in PENDING_LINK" };

  return {
    data: {
      uid,
      otp,
      agent_uid,
      device_id: deviceId,
    },
  };
}

export function parseSyncRequest(
  raw: string,
  terminalId: string
): ParseResult<SyncRequestData> {
  let deviceId = terminalId;
  let body = raw.trim();

  // Format 1: SYNC_REQUEST:device,[0,]timestamp,driver_uid,location
  if (body.startsWith("SYNC_REQUEST:")) {
    const after = body.substring("SYNC_REQUEST:".length).trim();
    const parts = after.split(",").map((p) => p.trim());
    if (parts.length === 4 || parts.length === 5) {
      deviceId = parts[0].toUpperCase();
      const timestampRaw = parts.length === 5 ? parts[2] : parts[1];
      const driver_uid = parts.length === 5 ? parts[3] : parts[2];
      const locationRaw = parts.length === 5 ? parts[4] : parts[3];

      const timestamp = parseInt(timestampRaw, 10);
      if (isNaN(timestamp) || timestamp <= 0) {
        return { error: "Invalid timestamp in SYNC_REQUEST" };
      }
      if (!driver_uid) return { error: "driver_uid is empty in SYNC_REQUEST" };
      const location = locationRaw ? locationRaw.toUpperCase() : "";
      if (location !== "A" && location !== "B" && location !== "C") {
        return { error: "Invalid location code in SYNC_REQUEST. Expected A, B, or C" };
      }
      return {
        data: { device_id: deviceId, timestamp, driver_uid, location },
      };
    }
  }

  // Format 2: {device_id}:SYNC_REQUEST,0,{unix_timestamp},{driver_uid},{location}
  if (body.includes(":") && body.indexOf(":") < body.indexOf("SYNC_REQUEST")) {
    const colonIdx = body.indexOf(":");
    const prefix = body.substring(0, colonIdx).trim();
    if (prefix) deviceId = prefix.toUpperCase();
    body = body.substring(colonIdx + 1).trim();
  }

  const parts = body.split(",").map((p) => p.trim());
  if (parts.length < 5 || parts[0] !== "SYNC_REQUEST") {
    return {
      error: `SYNC_REQUEST expects 5 fields (SYNC_REQUEST,0,timestamp,driver_uid,location), got ${parts.length}`,
    };
  }

  const [, , timestampRaw, driver_uid, locationRaw] = parts;

  if (!timestampRaw) return { error: "Missing timestamp in SYNC_REQUEST" };
  const timestamp = parseInt(timestampRaw, 10);
  if (isNaN(timestamp) || timestamp <= 0) {
    return { error: "Invalid timestamp in SYNC_REQUEST" };
  }

  if (!driver_uid || driver_uid.length === 0) {
    return { error: "driver_uid is empty in SYNC_REQUEST" };
  }

  const location = locationRaw ? locationRaw.toUpperCase() : "";
  if (location !== "A" && location !== "B" && location !== "C") {
    return { error: "Invalid location code in SYNC_REQUEST. Expected A, B, or C" };
  }

  return {
    data: { device_id: deviceId, timestamp, driver_uid, location },
  };
}

// Normalize legacy downlink commands
export function normalizeDownlinkCommand(cmd: string): string {
  const trimmed = cmd.trim();
  if (trimmed.startsWith("CMD:OTA,")) {
    return `SYS:OTA,${trimmed.substring("CMD:OTA,".length)}`;
  }
  return trimmed;
}

export function buildDeltaCommand(
  action: string,
  target: string,
  uid: string,
  extra: string | null = null
): string {
  const base = `${action}:${target},${uid}`;
  return extra ? `${base},${extra}` : base;
}

/**
 * Splits an array of UIDs into chunks with a maximum byte size.
 * Used for SYS:WL and SYS:BL commands to respect MTU limits.
 */
export function buildChunks(
  prefix: string,
  uids: string[],
  maxBytes: number
): string[] {
  const chunks: string[] = [];
  let currentChunk: string[] = [];
  let currentSize = prefix.length;

  for (const uid of uids) {
    const segment = currentChunk.length === 0 ? uid : `|${uid}`;
    if (currentSize + segment.length > maxBytes) {
      chunks.push(`${prefix}${currentChunk.join("|")}`);
      currentChunk = [uid];
      currentSize = prefix.length + uid.length;
    } else {
      currentChunk.push(uid);
      currentSize += segment.length;
    }
  }

  if (currentChunk.length > 0) {
    chunks.push(`${prefix}${currentChunk.join("|")}`);
  }

  return chunks;
}