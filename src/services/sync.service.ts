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

async function enqueueTerminalCommand(
  terminalId: string,
  command: string
): Promise<boolean> {
  const normId = terminalId.toUpperCase();
  const redis = getRedisClient();
  const queueKey = redisKeys.terminalQueue(normId);

  try {
    const existing = await redis.lrange(queueKey, 0, -1);

    // New System state command update
    // SYS:OTA - is a controlled command
    const statePrefixes = [
      "SYS:LOC,",
      "SYS:FARE,",
      "SYS:FARE_A,",
      "SYS:FARE_B,",
      "SYS:FARE_C,",
      "SYS:OTA,",
    ];
    const matchingPrefix = statePrefixes.find((p) => command.startsWith(p));

    if (matchingPrefix) {
      for (const item of existing) {
        if (item.startsWith(matchingPrefix)) {
          await redis.lrem(queueKey, 0, item);
          logger.info(
            { terminalId: normId, staleCommand: item, newCommand: command },
            "sync.enqueue_replaced_stale_state_command"
          );
        }
      }
    } else if (existing.includes(command)) {
      logger.debug(
        { terminalId: normId, command },
        "sync.enqueue_skipped_duplicate — command already queued"
      );
      return false;
    }

    await redis.rpush(queueKey, command);
    logger.info(
      { terminalId: normId, command },
      "sync.command_queued_loss_resistant"
    );
    return true;
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    logger.error(
      { terminalId: normId, command, err: errMsg },
      "sync.enqueue_command_error"
    );
    throw err;
  }
}

async function routeDeltaToTerminal(
  terminalId: string,
  deltaCommand: string
): Promise<void> {
  const normId = terminalId.toUpperCase();
  const redis = getRedisClient();
  const log = logger.child({ terminalId: normId, deltaCommand });
  const queueKey = redisKeys.terminalQueue(normId);

  try {
    const terminal = await prisma.terminal.findUnique({
      where: { terminal_id: normId },
      select: { status: true },
    });

    if (!terminal) {
      log.warn("sync.terminal_not_in_db — queuing delta");
      await enqueueTerminalCommand(normId, deltaCommand);
      return;
    }

    const pendingCount = await redis.llen(queueKey);

    // If terminal is marked ONLINE, has no queued backlog, and MQTT publisher is ready:
    if (terminal.status === "ONLINE" && pendingCount === 0 && _mqttPublish) {
      try {
        await _mqttPublish(normId, deltaCommand);
        log.info("sync.delta_published_direct");
        return;
      } catch (pubErr) {
        const errMsg =
          pubErr instanceof Error ? pubErr.message : "Unknown error";
        log.warn(
          { err: errMsg },
          "sync.delta_direct_publish_failed — queuing command for retry"
        );
        await enqueueTerminalCommand(normId, deltaCommand);
        return;
      }
    }

    // Terminal is OFFLINE, publisher missing, or has existing backlog to preserve FIFO:
    await enqueueTerminalCommand(normId, deltaCommand);
    log.info(
      { terminalStatus: terminal.status, pendingCount },
      "sync.delta_queued_redis"
    );

    // If terminal is marked ONLINE and publisher ready, trigger queue flush
    if (terminal.status === "ONLINE" && _mqttPublish) {
      flushTerminalQueue(normId).catch((flushErr) => {
        log.error({ err: flushErr }, "sync.flush_queue_trigger_error");
      });
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.route_delta_error — queuing as fallback");
    try {
      await enqueueTerminalCommand(normId, deltaCommand);
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
        const targetId = t.terminal_id.toUpperCase();
        const qLog = log.child({ terminalId: targetId });
        const queueKey = redisKeys.terminalQueue(targetId);

        try {
          const pendingCount = await redis.llen(queueKey);
          if (t.status === "ONLINE" && pendingCount === 0 && _mqttPublish) {
            try {
              await _mqttPublish(targetId, deltaCommand);
              qLog.debug("sync.broadcast_published_direct");
              return;
            } catch (pubErr) {
              const errMsg =
                pubErr instanceof Error ? pubErr.message : "Unknown error";
              qLog.warn(
                { err: errMsg },
                "sync.broadcast_direct_publish_failed — queuing to redis"
              );
              await enqueueTerminalCommand(targetId, deltaCommand);
              return;
            }
          }

          await enqueueTerminalCommand(targetId, deltaCommand);
          qLog.debug("sync.broadcast_queued_redis");

          if (t.status === "ONLINE" && _mqttPublish) {
            flushTerminalQueue(targetId).catch((err) => {
              qLog.error({ err }, "sync.broadcast_flush_error");
            });
          }
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : "Unknown error";
          qLog.error({ err: errMsg }, "sync.broadcast_routing_failed");
          try {
            await enqueueTerminalCommand(targetId, deltaCommand);
          } catch {
            // ignore
          }
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

const activeFlushes = new Set<string>();

async function flushTerminalQueue(terminalId: string): Promise<void> {
  const normId = terminalId.toUpperCase();
  if (activeFlushes.has(normId)) {
    logger.debug(
      { terminalId: normId },
      "sync.flush_queue_already_in_progress"
    );
    return;
  }
  activeFlushes.add(normId);

  const redis = getRedisClient();
  const log = logger.child({ terminalId: normId });

  if (!_mqttPublish) {
    log.error("sync.flush_queue_no_mqtt_publisher");
    activeFlushes.delete(normId);
    return;
  }

  const queueKey = redisKeys.terminalQueue(normId);

  try {
    const queueLength = await redis.llen(queueKey);
    if (queueLength === 0) {
      log.debug("sync.flush_queue_empty");
      return;
    }

    log.info({ queueLength }, "sync.flush_queue_start");

    let flushedCount = 0;

    // Loss-resistant delivery process:
    // 1. Peek at head (index 0) WITHOUT removing it from Redis.
    // 2. Publish to MQTT terminal downlink topic.
    // 3. ONLY once MQTT publish succeeds, remove the item from the head of the queue.
    // 4. If MQTT publish fails, the command remains safely at index 0 for future retry.
    while (true) {
      const command = await redis.lindex(queueKey, 0);
      if (!command) {
        break; // Queue is empty
      }

      try {
        await _mqttPublish(normId, command);

        // Successful publish: atomically pop from the head of the queue exactly once
        await redis.lpop(queueKey);
        flushedCount++;
        log.debug({ command, flushedCount }, "sync.flush_queue_item_sent");
      } catch (publishErr) {
        const errMsg =
          publishErr instanceof Error ? publishErr.message : "Unknown error";
        log.error(
          { command, err: errMsg },
          "sync.flush_queue_publish_failed — command preserved in queue for retry"
        );
        // Do NOT pop! The command remains at index 0 in Redis for next retry.
        break;
      }
    }

    log.info({ flushedCount }, "sync.flush_queue_complete");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.flush_queue_error");
  } finally {
    activeFlushes.delete(normId);
  }
}

/// Send WL to specific terminals
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
  enqueueTerminalCommand,
  routeDeltaToTerminal,
  broadcastDeltaToFleet,
  flushTerminalQueue,
  syncFullListsToTerminal,
};
