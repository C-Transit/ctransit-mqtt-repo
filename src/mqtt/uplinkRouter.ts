// src/mqtt/uplinkRouter.ts
import {
  PAYLOAD_TYPE,
  detectPayloadType,
  splitBatchPayload,
  parsePendingLink,
  parseSyncRequest,
  parseDriverRegister,
  parseDriverEvent,
  type DriverEventData,
  type DriverRegisterData,
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
  handleDriverRegister,
  handleDriverLogin,
  handleDriverLogout,
} from "../services/driver.service.js";

type ChildLogger = Logger;

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

  const { deviceId, lines } = splitBatchPayload(rawMessage, terminalId);

  // The topic is the only identity the broker vouches for. A device id written inside
  // the payload must match it, otherwise one terminal could act as another.
  if (deviceId !== terminalId) {
    log.warn(
      { claimedDeviceId: deviceId },
      "uplink.device_id_topic_mismatch — discarding"
    );
    return;
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

    case PAYLOAD_TYPE.DRIVER_REGISTER: {
      const { data, error } = parseDriverRegister(line);
      if (error) {
        log.warn({ error }, "uplink.driver_register_parse_error — discarding");
        break;
      }
      if (data) {
        await handleDriverRegisterEvent(terminalId, data, log);
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

async function handleDriverRegisterEvent(
  terminalId: string,
  data: DriverRegisterData,
  log: ChildLogger
): Promise<void> {
  const result = await handleDriverRegister(terminalId, data);

  if (result.success) {
    await publishToTerminal(terminalId, `DRV:REG_OK,${data.matricNumber}`);
    log.info({ matricNumber: data.matricNumber }, "uplink.driver_registered");
  } else {
    await publishToTerminal(terminalId, `DRV:REG_FAIL,${result.message}`);
    log.warn(
      { matricNumber: data.matricNumber },
      "uplink.driver_register_failed"
    );
  }
}

export { routeUplinkMessage };