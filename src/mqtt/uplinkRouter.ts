// src/mqtt/uplinkRouter.ts
import {
  PAYLOAD_TYPE,
  detectPayloadType,
  splitBatchPayload,
  parsePendingLink,
  parseSyncRequest,
  parseDriverEvent,
  type DriverEventData,
  type SyncRequestData,
} from "../utils/parser.js";
import { ingestTransactionBatch } from "../services/ingestion.service.js";
import { handlePendingLink } from "../services/registration.service.js";
import { syncFullListsToTerminal } from "../services/sync.service.js";
import { handleLwtEvent } from "../mqtt/lwtHandler.js";

import logger from "../config/logger.js";
import type { Logger } from "pino";
import { publishToTerminal } from "./downlinkQueue.js";
import {
  handleDriverLogin,
  handleDriverLogout,
} from "../services/driver.service.js";

type ChildLogger = Logger;

// "CMD:<12 hex>;" is an acknowledgement of a wrapped fleet command.
const CMD_ACK_PATTERN = /^CMD:([0-9a-f]{12});/;

const errText = (err: unknown): string =>
  err instanceof Error ? err.message : "Unknown error";

function extractTerminalId(topic: string): string | null {
  const parts = topic.split("/");
  if (parts.length !== 3 || parts[0] !== "ctransit") return null;

  const id = parts[1].toUpperCase();

  // Very Important: Prevent the server from treating itself as a terminal
  if (id === "SERVER") return null;

  return id;
}

async function routeUplinkMessage(
  topic: string,
  payloadBuffer: Buffer
): Promise<void> {
  // The server now also subscribes to rx; those messages are not uplinks.
  if (topic.endsWith("/rx")) {
    logger.debug({ topic }, "uplink.rx_topic_ignored");
    return;
  }

  const terminalId = extractTerminalId(topic);
  if (!terminalId) {
    logger.warn({ topic }, "uplink.unrecognised_topic_format — discarding");
    return;
  }

  const log = logger.child({ terminalId, topic });
  const rawMessage = payloadBuffer.toString("utf8").trim();

  if (topic.endsWith("/status")) {
    await handleLwtEvent(terminalId, rawMessage);
    return;
  }

  if (!topic.endsWith("/tx")) {
    log.warn("uplink.unexpected_topic_suffix — discarding");
    return;
  }

  // Records are separated by newlines. Acks are handled line by line first, so a
  // "CMD:<id>;..." record is never split on "|" or treated as tap data.
  const records = rawMessage
    .split(/\r?\n/)
    .map((r) => r.trim())
    .filter((r) => r.length > 0);

  for (const record of records) {
    const ack = CMD_ACK_PATTERN.exec(record);
    if (ack) {
      log.debug(
        {
          commandId: ack[1],
          terminalId,
          innerLength: record.length - ack[0].length,
        },
        "uplink.ack_received"
      );
      continue;
    }

    const { deviceId, lines } = splitBatchPayload(record, terminalId);

    // The topic is the only identity the broker vouches for. A device id written inside
    // the payload must match it, otherwise one terminal could act as another.
    if (deviceId !== terminalId) {
      log.warn(
        { claimedDeviceId: deviceId },
        "uplink.device_id_topic_mismatch — discarding"
      );
      continue;
    }

    log.debug({ messageCount: lines.length }, "uplink.batch_split_complete");

    // Each line is handled on its own: one failing line must not drop the rest.
    for (const line of lines) {
      try {
        await handleLine(terminalId, line, log);
      } catch (err) {
        log.error({ err: errText(err) }, "uplink.line_processing_failed");
      }
    }
  }
}

async function handleLine(
  terminalId: string,
  line: string,
  log: ChildLogger
): Promise<void> {
  const payloadType = detectPayloadType(line);
  log.debug({ payloadType, lineLength: line.length }, "uplink.message_received");

  switch (payloadType) {
    case PAYLOAD_TYPE.TRANSACTION_BATCH:
      await ingestTransactionBatch(terminalId, line);
      break;

    case PAYLOAD_TYPE.PENDING_LINK: {
      const { data, error } = parsePendingLink(line, terminalId);
      if (error) {
        log.warn({ error }, "uplink.pending_link_parse_error — discarding");
        break;
      }
      if (data) {
        if (data.device_id && data.device_id.toUpperCase() !== terminalId) {
          log.warn("uplink.pending_link_device_mismatch — discarding");
          break;
        }
        await handlePendingLink(terminalId, data);
      }
      break;
    }

    case PAYLOAD_TYPE.SYNC_REQUEST: {
      const { data, error } = parseSyncRequest(line, terminalId);
      if (error) {
        log.warn({ error }, "uplink.sync_request_parse_error — discarding");
        break;
      }
      if (data) {
        await handleSyncRequest(terminalId, data, log);
      }
      break;
    }

    case PAYLOAD_TYPE.SYS_FULL_SYNC:
      await syncFullListsToTerminal(terminalId);
      break;

    case PAYLOAD_TYPE.DRIVER_EVENT: {
      const { data, error } = parseDriverEvent(line);
      if (error) {
        log.warn({ error }, "uplink.driver_event_parse_error — discarding");
        break;
      }
      if (data) {
        await handleDriverEvent(terminalId, data, log);
      }
      break;
    }

    default:
      log.warn({ lineLength: line.length }, "uplink.unknown_payload_type");
      break;
  }
}

async function handleSyncRequest(
  terminalId: string,
  data: SyncRequestData,
  log: ChildLogger
): Promise<void> {
  // A terminal can only ask for its own sync.
  if (data.device_id && data.device_id.toUpperCase() !== terminalId) {
    log.warn("uplink.sync_request_device_mismatch — discarding");
    return;
  }

  log.info(
    { timestamp: data.timestamp, location: data.location },
    "uplink.sync_request_received"
  );
  await syncFullListsToTerminal(terminalId);
}

async function handleDriverEvent(
  terminalId: string,
  data: DriverEventData,
  log: ChildLogger
): Promise<void> {
  const { action, driverUid } = data;

  if (action === "LOGIN") {
    const result = await handleDriverLogin(terminalId, driverUid as string);

    if (result.success) {
      await publishToTerminal(terminalId, `DRV:OK,${result.driverName}`);
      log.info({ driverUid }, "uplink.driver_login_success");
    } else {
      await publishToTerminal(terminalId, `DRV:FAIL,${result.message}`);
      log.warn({ driverUid }, "uplink.driver_login_failed");
    }
  } else if (action === "LOGOUT") {
    await handleDriverLogout(terminalId, driverUid as string);
    await publishToTerminal(terminalId, "DRV:BYE");
    log.info({ driverUid }, "uplink.driver_logout");
  }
}

export { routeUplinkMessage };