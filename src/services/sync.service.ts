// src/services/sync.service.ts — full replacement
"use strict";

import { getRedisClient, redisKeys } from "../config/redis.js";
import logger from "../config/logger.js";
import { prisma } from "./ledger.service.js";
import { buildChunks } from "../utils/parser.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";

export type MqttPublishFn = (
  terminalId: string,
  message: string
) => Promise<void>;

let _mqttPublish: MqttPublishFn | null = null;

function injectMqttPublisher(publishFn: MqttPublishFn): void {
  _mqttPublish = publishFn;
}

async function routeDeltaToTerminal(
  terminalId: string,
  deltaCommand: string
): Promise<void> {
  const redis = getRedisClient();
  const log = logger.child({ terminalId, deltaCommand });

  try {
    const terminal = await prisma.terminal.findUnique({
      where: { terminal_id: terminalId },
      select: { status: true },
    });

    if (!terminal) {
      log.warn("sync.terminal_not_in_db — queuing delta");
      await redis.rpush(redisKeys.terminalQueue(terminalId), deltaCommand);
      return;
    }

    if (terminal.status === "ONLINE") {
      if (!_mqttPublish) {
        log.error("sync.mqtt_publisher_not_injected — falling back to queue");
        await redis.rpush(redisKeys.terminalQueue(terminalId), deltaCommand);
        return;
      }
      await _mqttPublish(terminalId, deltaCommand);
      log.info("sync.delta_published_direct");
    } else {
      await redis.rpush(redisKeys.terminalQueue(terminalId), deltaCommand);
      log.info({ terminalStatus: terminal.status }, "sync.delta_queued_redis");
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.route_delta_error — queuing as fallback");
    try {
      await redis.rpush(redisKeys.terminalQueue(terminalId), deltaCommand);
    } catch (redisErr) {
      const redisErrMsg =
        redisErr instanceof Error ? redisErr.message : "Unknown error";
      log.error({ err: redisErrMsg }, "sync.redis_fallback_queue_failed");
    }
  }
}

async function broadcastDeltaToFleet(deltaCommand: string): Promise<void> {
  const log = logger.child({ deltaCommand });
  const redis = getRedisClient();

  try {
    // Blacklist cache removed — no cache invalidation needed here.
    // DB is now the source of truth for blacklist status on every tap.

    const terminals = await prisma.terminal.findMany({
      select: { terminal_id: true, status: true },
    });

    if (terminals.length === 0) {
      log.warn("sync.fleet_broadcast_no_terminals");
      return;
    }

    log.info({ terminalCount: terminals.length }, "sync.fleet_broadcast_start");

    const promises = terminals.map(
      async (t: { terminal_id: string; status: string }) => {
        const qLog = log.child({ terminalId: t.terminal_id });
        try {
          if (t.status === "ONLINE" && _mqttPublish) {
            await _mqttPublish(t.terminal_id, deltaCommand);
            qLog.debug("sync.broadcast_published_direct");
          } else {
            await redis.rpush(
              redisKeys.terminalQueue(t.terminal_id),
              deltaCommand
            );
            qLog.debug("sync.broadcast_queued_redis");
          }
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : "Unknown error";
          qLog.error({ err: errMsg }, "sync.broadcast_routing_failed");
        }
      }
    );

    await Promise.allSettled(promises);
    log.info("sync.fleet_broadcast_complete");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.fleet_broadcast_error");
  }
}

async function flushTerminalQueue(terminalId: string): Promise<void> {
  const redis = getRedisClient();
  const log = logger.child({ terminalId });

  if (!_mqttPublish) {
    log.error("sync.flush_queue_no_mqtt_publisher");
    return;
  }

  const queueKey = redisKeys.terminalQueue(terminalId);

  try {
    const queueLength = await redis.llen(queueKey);
    if (queueLength === 0) {
      log.debug("sync.flush_queue_empty");
      return;
    }

    log.info({ queueLength }, "sync.flush_queue_start");

    let command: string | null;
    let flushedCount = 0;

    while ((command = await redis.lpop(queueKey)) !== null) {
      try {
        await _mqttPublish(terminalId, command);
        flushedCount++;
        log.debug({ command, flushedCount }, "sync.flush_queue_item_sent");
      } catch (publishErr) {
        const errMsg =
          publishErr instanceof Error ? publishErr.message : "Unknown error";
        await redis.lpush(queueKey, command);
        log.error(
          { command, err: errMsg },
          "sync.flush_queue_publish_failed — re-queued at front"
        );
        break;
      }
    }
    log.info({ flushedCount }, "sync.flush_queue_complete");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.flush_queue_error");
  }
}

/**
  * Sends the full whitelist (all card UIDs) and blacklist (card UIDs of blacklisted students)
  * to a specific terminal.
  * 
  * Order: BL first, then WL, then SYNC_COMPLETE.
  */
async function syncFullListsToTerminal(terminalId: string): Promise<void> {
  const log = logger.child({ terminalId });
  const MAX_CHUNK_BYTES = 500;
  const SYNC_DELAY_MS = 200; // Paced delay for microcontroller buffer
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  try {
    // 1. Fetch whitelist and blacklist in parallel to reduce DB latency
    const [cardMappings, blacklistedStudents] = await Promise.all([
      prisma.cardMapping.findMany({
        select: { card_uid: true },
      }),
      prisma.blacklist.findMany({
        select: { student_uid: true },
      }),
    ]);

    const whitelistUids = cardMappings.map((c) => c.card_uid);

    let blacklistUids: string[] = [];
    if (blacklistedStudents.length > 0) {
      const studentUids = blacklistedStudents.map((b) => b.student_uid);
      const blacklistCards = await prisma.cardMapping.findMany({
        where: { student_uid: { in: studentUids } },
        select: { card_uid: true },
      });
      blacklistUids = blacklistCards.map((c) => c.card_uid);
    }

    log.info(
      { wlCount: whitelistUids.length, blCount: blacklistUids.length },
      "sync.full_lists_fetched"
    );

    // 2. Send BL first
    if (blacklistUids.length === 0) {
      await publishToTerminal(terminalId, "SYS:BL,EMPTY");
    } else {
      const blChunks = buildChunks("SYS:BL,", blacklistUids, MAX_CHUNK_BYTES);
      for (let i = 0; i < blChunks.length; i++) {
        await publishToTerminal(terminalId, blChunks[i]);
        log.debug(
          { chunk: i + 1, total: blChunks.length },
          "sync.bl_chunk_sent"
        );
        if (i < blChunks.length - 1) await sleep(SYNC_DELAY_MS);
      }
    }

    await sleep(SYNC_DELAY_MS);

    // 3. Send WL
    if (whitelistUids.length === 0) {
      await publishToTerminal(terminalId, "SYS:WL,EMPTY");
    } else {
      const wlChunks = buildChunks("SYS:WL,", whitelistUids, MAX_CHUNK_BYTES);
      for (let i = 0; i < wlChunks.length; i++) {
        await publishToTerminal(terminalId, wlChunks[i]);
        log.debug(
          { chunk: i + 1, total: wlChunks.length },
          "sync.wl_chunk_sent"
        );
        if (i < wlChunks.length - 1) await sleep(SYNC_DELAY_MS);
      }
    }

    await sleep(SYNC_DELAY_MS);

    // 4. Signal completion
    await publishToTerminal(terminalId, "SYS:SYNC_COMPLETE");
    log.info("sync.full_sync_complete");
  } catch (error) {
    const errMessage =
      error instanceof Error ? error.message : "Unknown error";
    log.error({ err: errMessage }, "sync.full_sync_error");
    throw error;
  }
}

export {
  injectMqttPublisher,
  routeDeltaToTerminal,
  broadcastDeltaToFleet,
  flushTerminalQueue,
  syncFullListsToTerminal,
};
