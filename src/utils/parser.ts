// Update V1.1.0 parser for terminal payload
"use strict";

// Utility type for strict error handling across all parsers
export type ParseResult<T> =
  | { error: string; data?: undefined }
  | { data: T; error?: undefined };

export const PAYLOAD_TYPE = {
  TRANSACTION_BATCH: "TRANSACTION_BATCH",
  PENDING_LINK: "PENDING_LINK",
  SYNC_REQUEST: "SYNC_REQUEST",
  SYS_FULL_SYNC: "SYS_FULL_SYNC",
  DRIVER_EVENT: "DRIVER_EVENT",
  DRIVER_REGISTER: "DRIVER_REGISTER",
  UNKNOWN: "UNKNOWN",
} as const;

export type PayloadType = (typeof PAYLOAD_TYPE)[keyof typeof PAYLOAD_TYPE];

export const VALID_LOCATIONS = ["A", "B", "C"] as const;
export type ValidLocation = (typeof VALID_LOCATIONS)[number];

export const LOCATION_NAMES: Record<ValidLocation, string> = {
  A: "Bus Park",
  B: "Department",
  C: "Hostel/Clinic",
};

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

export interface DriverRegisterData {
  firstname: string;
  lastname: string;
  matricNumber: string;
}

export interface BatchSplitResult {
  deviceId: string;
  lines: string[];
}

// Splits into different batch before matching
export function splitBatchPayload(
  raw: string,
  fallbackTerminalId: string
): BatchSplitResult {
  const trimmed = raw.trim();
  let deviceId = fallbackTerminalId;
  let body = trimmed;

  // Extract leading device_id if present before a colon: e.g. "8HY1K:..."
  if (
    body.includes(":") &&
    !body.startsWith("PENDING_LINK:") &&
    !body.startsWith("SYNC_REQUEST") &&
    !body.startsWith("SYS:") &&
    !body.startsWith("DRV:")
  ) {
    const colonIdx = body.indexOf(":");
    const possibleDevice = body.substring(0, colonIdx).trim();
    // Device ID is alphanumeric with no commas
    if (possibleDevice && !possibleDevice.includes(",") && !possibleDevice.includes("|")) {
      deviceId = possibleDevice.toUpperCase();
      body = body.substring(colonIdx + 1).trim();
    }
  }

  // Split on pipe (|) or newline (\n)
  const lines = body
    .split(/[\n|]/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  return { deviceId, lines };
}

export function detectPayloadType(raw: string): PayloadType {
  let trimmed = raw.trim();

  // Strip leading device prefix if present on individual line
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
  if (trimmed.startsWith("DRV:REG,")) return PAYLOAD_TYPE.DRIVER_REGISTER;
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
    return {
      error: `Unknown DRV action: "${action}". Expected LOGIN or LOGOUT`,
    };
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

export function parseSingleTransaction(
  line: string,
  terminalId: string
): ParseResult<ParsedTransaction> {
  let actualTerminalId = terminalId;
  let body = line.trim();

  // Strip device prefix if present
  if (
    body.includes(":") &&
    !body.startsWith("PENDING_LINK") &&
    !body.startsWith("SYNC_REQUEST")
  ) {
    const colonIdx = body.indexOf(":");
    const possibleDevice = body.substring(0, colonIdx).trim();
    if (possibleDevice && !possibleDevice.includes(",")) {
      actualTerminalId = possibleDevice.toUpperCase();
      body = body.substring(colonIdx + 1).trim();
    }
  }

  const parts = body.split(",").map((p) => p.trim());

  let transaction_id: string | undefined;
  let student_uid: string | undefined;
  let amountRaw: string | undefined;
  let timestampRaw: string | undefined;
  let driver_uid: string | undefined;
  let location: string | null = null;

  if (parts.length === 5) {
    const p1IsNum = !isNaN(parseFloat(parts[1]));
    const p2IsTs = !isNaN(parseInt(parts[2], 10)) && parseInt(parts[2], 10) > 1000000;

    if (p1IsNum && p2IsTs) {

      // v1.1.0L Ride format
      [student_uid, amountRaw, timestampRaw, driver_uid] = parts;
      const locationCode = parts[4].trim().toUpperCase();

      // Validate location against allowed set {A, B, C}
      if (
        locationCode !== "A" &&
        locationCode !== "B" &&
        locationCode !== "C"
      ) {
        return {
          error: `Invalid location code: "${parts[4]}". Expected A, B, or C`,
        };
      }
      location = locationCode;

      if (!timestampRaw) return { error: "Missing timestamp field" };
      const ts = parseInt(timestampRaw, 10);
      transaction_id = `${actualTerminalId}-${ts}-${student_uid}`;
    } else {

      // Legacy Format A
      [transaction_id, student_uid, amountRaw, timestampRaw, driver_uid] =
        parts;
    }
  } else if (parts.length >= 3 && parts.length <= 4) {

    // Legacy Format B
    [student_uid, amountRaw, timestampRaw, driver_uid] = parts;

    if (!timestampRaw) return { error: "Missing timestamp field" };
    const ts = parseInt(timestampRaw, 10);
    transaction_id = `${actualTerminalId}-${ts}-${student_uid}`;
  } else {
    return { error: `Expected 3-5 CSV fields, got ${parts.length}` };
  }

  // Validate transaction_id
  if (!transaction_id || transaction_id.length === 0) {
    return { error: "transaction_id is empty" };
  }

  // Validate student_uid (physical NFC Card UID)
  if (!student_uid || student_uid.length === 0) {
    return { error: "student_uid is empty" };
  }

  // Validate amount
  if (!amountRaw) return { error: "amount is missing" };
  const parsedAmount = parseFloat(amountRaw);
  if (isNaN(parsedAmount) || parsedAmount === 0) {
    return { error: `Invalid amount: "${amountRaw}"` };
  }
  const amount = Math.abs(parsedAmount);

  // Validate timestamp
  if (!timestampRaw) return { error: "timestamp is missing" };
  const epochSeconds = parseInt(timestampRaw, 10);
  if (isNaN(epochSeconds) || epochSeconds <= 0) {
    return { error: `Invalid timestamp: "${timestampRaw}"` };
  }
  const synced_at = new Date(epochSeconds * 1000);

  return {
    data: {
      transaction_id,
      terminal_id: actualTerminalId,
      student_uid,
      card_uid: student_uid,
      amount,
      raw_amount: parsedAmount,
      timestamp: epochSeconds,
      driver_uid: driver_uid || null,
      location,
      synced_at,
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

  if (!uid || uid.length === 0)
    return { error: "UID is empty in PENDING_LINK" };
  if (!otp || !/^\d{6}$/.test(otp))
    return { error: `OTP must be 6 digits, got "${otp}"` };
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

  // Format 1: SYNC_REQUEST
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
        return { error: `Invalid timestamp in SYNC_REQUEST: "${timestampRaw}"` };
      }
      if (!driver_uid) return { error: "driver_uid is empty in SYNC_REQUEST" };
      const location = locationRaw ? locationRaw.toUpperCase() : "";
      if (location !== "A" && location !== "B" && location !== "C") {
        return {
          error: `Invalid location code in SYNC_REQUEST: "${locationRaw}". Expected A, B, or C`,
        };
      }
      return {
        data: {
          device_id: deviceId,
          timestamp,
          driver_uid,
          location,
        },
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

  // Validate timestamp
  if (!timestampRaw) {
    return { error: "Missing timestamp in SYNC_REQUEST" };
  }
  const timestamp = parseInt(timestampRaw, 10);
  if (isNaN(timestamp) || timestamp <= 0) {
    return { error: `Invalid timestamp in SYNC_REQUEST: "${timestampRaw}"` };
  }

  // Validate driver_uid
  if (!driver_uid || driver_uid.length === 0) {
    return { error: "driver_uid is empty in SYNC_REQUEST" };
  }

  // Validate location against allowed set {A, B, C}
  const location = locationRaw ? locationRaw.toUpperCase() : "";
  if (location !== "A" && location !== "B" && location !== "C") {
    return {
      error: `Invalid location code in SYNC_REQUEST: "${locationRaw}". Expected A, B, or C`,
    };
  }

  return {
    data: {
      device_id: deviceId,
      timestamp,
      driver_uid,
      location,
    },
  };
}

export function parseDriverRegister(
  raw: string
): ParseResult<DriverRegisterData> {
  let body = raw.trim();
  if (body.includes("DRV:REG,")) {
    body = body.substring(body.indexOf("DRV:REG,") + 8).trim();
  }
  const parts = body.split(",").map((p) => p.trim());

  if (parts.length !== 3) {
    return {
      error: `DRV:REG expects 3 fields (firstname,lastname,matricNumber), got ${parts.length}`,
    };
  }

  const [firstname, lastname, matricNumber] = parts;

  if (!firstname) return { error: "firstname is empty" };
  if (!lastname) return { error: "lastname is empty" };
  if (!matricNumber) return { error: "matricNumber is empty" };

  return { data: { firstname, lastname, matricNumber } };
}

// Downlink Command Builders & Normalizers
export function buildFareCommand(
  location: "A" | "B" | "C" | null,
  amount: number
): string {
  if (location) {
    return `SYS:FARE_${location},${amount}`;
  }
  return `SYS:FARE,${amount}`;
}

export function buildLocationCommand(location: "A" | "B" | "C"): string {
  return `SYS:LOC,${location}`;
}

export function buildOtaCommand(url: string): string {
  return `SYS:OTA,${url}`;
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