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

// Very Important: Handle LWT messages first, before any other processing
  if (topic.endsWith("/status")) {
    await handleLwtEvent(terminalId, rawMessage);
    return;
  }

  if (!topic.endsWith("/tx")) {
    log.warn("uplink.unexpected_topic_suffix — discarding");
    return;
  }

  // Very Important: Split the batch payload into individual lines, and process each line
  const { deviceId, lines } = splitBatchPayload(rawMessage, terminalId);
  const activeTerminalId = deviceId || terminalId;

  log.debug(
    { messageCount: lines.length, activeTerminalId },
    "uplink.batch_split_complete"
  );

  for (const line of lines) {
    const payloadType = detectPayloadType(line);
    log.debug(
      { payloadType, lineLength: line.length },
      "uplink.message_received"
    );

    switch (payloadType) {
      case PAYLOAD_TYPE.TRANSACTION_BATCH:
        await ingestTransactionBatch(activeTerminalId, line);
        break;

      case PAYLOAD_TYPE.PENDING_LINK: {
        const { data, error } = parsePendingLink(line, activeTerminalId);
        if (error) {
          log.warn({ error, line }, "uplink.pending_link_parse_error — discarding");
          break;
        }
        if (data) {
          await handlePendingLink(activeTerminalId, data);
        }
        break;
      }

      case PAYLOAD_TYPE.SYNC_REQUEST: {
        const { data, error } = parseSyncRequest(line, activeTerminalId);
        if (error) {
          log.warn({ error, line }, "uplink.sync_request_parse_error — discarding");
          break;
        }
        if (data) {
          await handleSyncRequest(activeTerminalId, data, log);
        }
        break;
      }

      case PAYLOAD_TYPE.SYS_FULL_SYNC:
        await syncFullListsToTerminal(activeTerminalId);
        break;

      case PAYLOAD_TYPE.DRIVER_EVENT: {
        const { data, error } = parseDriverEvent(line);
        if (error) {
          log.warn({ error, line }, "uplink.driver_event_parse_error — discarding");
          break;
        }
        if (data) {
          await handleDriverEvent(activeTerminalId, data, log);
        }
        break;
      }

      case PAYLOAD_TYPE.DRIVER_REGISTER: {
        const { data, error } = parseDriverRegister(line);
        if (error) {
          log.warn({ error, line }, "uplink.driver_register_parse_error — discarding");
          break;
        }
        if (data) {
          await handleDriverRegisterEvent(activeTerminalId, data, log);
        }
        break;
      }

      default:
        log.warn(
          { rawPayload: line.substring(0, 80) },
          "uplink.unknown_payload_type"
        );
        break;
    }
  }
}

// Very Important: Handle Sync Request messages by triggering a full sync to the terminal
async function handleSyncRequest(
  terminalId: string,
  data: SyncRequestData,
  log: ChildLogger
): Promise<void> {
  const targetTerminalId = data.device_id || terminalId;
  log.info(
    {
      terminalId: targetTerminalId,
      timestamp: data.timestamp,
      driver_uid: data.driver_uid,
      location: data.location,
    },
    "uplink.sync_request_received"
  );
  await syncFullListsToTerminal(targetTerminalId);
}

// async function handleFullSyncRequest(
//   terminalId: string,
//   log: ChildLogger
// ): Promise<void> {
//   log.info("uplink.full_sync_requested");

//   const MAX_CHUNK_BYTES = 500;
//   const SYNC_DELAY_MS = 2000;
//   const sleep = (ms: number) =>
//     new Promise((resolve) => setTimeout(resolve, ms));

//   try {
//     // Fetch whitelist — all linked wallets
//     const cardMappings = await prisma.cardMapping.findMany({
//       select: { card_uid: true },
//     });

//     const whitelistUids = cardMappings.map(
//       (c: { card_uid: string }) => c.card_uid
//     );

//     // Fetch blacklist — all blacklisted wallets
//     const blacklistedMappings = await prisma.cardMapping.findMany({
//       where: {
//         student_uid: {
//           in: await prisma.blacklist
//             .findMany({ select: { student_uid: true } })
//             .then((bl: { student_uid: string }[]) =>
//               bl.map((b: { student_uid: string }) => b.student_uid)
//             ),
//         },
//       },
//       select: { card_uid: true },
//     });

//     const blacklistUids = blacklistedMappings.map(
//       (c: { card_uid: string }) => c.card_uid
//     );

//     log.info(
//       {
//         whitelistCount: whitelistUids.length,
//         blacklistCount: blacklistUids.length,
//       },
//       "uplink.full_sync_data_fetched"
//     );

//     // Publish Whitelist
//     if (whitelistUids.length === 0) {
//       await publishToTerminal(terminalId, "SYS:WL,EMPTY");
//     } else {
//       const wlChunks = buildChunks("SYS:WL,", whitelistUids, MAX_CHUNK_BYTES);
//       for (let i = 0; i < wlChunks.length; i++) {
//         await publishToTerminal(terminalId, wlChunks[i]);
//         log.debug(
//           { chunk: i + 1, total: wlChunks.length },
//           "uplink.wl_chunk_sent"
//         );
//         if (i < wlChunks.length - 1) await sleep(SYNC_DELAY_MS);
//       }
//     }

//     await sleep(SYNC_DELAY_MS);

//     // Publish Blacklist
//     if (blacklistUids.length === 0) {
//       await publishToTerminal(terminalId, "SYS:BL,EMPTY");
//     } else {
//       const blChunks = buildChunks("SYS:BL,", blacklistUids, MAX_CHUNK_BYTES);
//       for (let i = 0; i < blChunks.length; i++) {
//         await publishToTerminal(terminalId, blChunks[i]);
//         log.debug(
//           { chunk: i + 1, total: blChunks.length },
//           "uplink.bl_chunk_sent"
//         );
//         if (i < blChunks.length - 1) await sleep(SYNC_DELAY_MS);
//       }
//     }

//     await sleep(SYNC_DELAY_MS);

//     await publishToTerminal(terminalId, "SYS:SYNC_COMPLETE");
//     log.info("uplink.full_sync_complete");
//   } catch (error) {
//     const errMessage = error instanceof Error ? error.message : "Unknown error";
//     log.error({ err: errMessage }, "uplink.full_sync_error");
//     throw error;
//   }
// }

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
