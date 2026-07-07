"use strict";

import { getRedisClient, redisKeys, cacheKeys } from "../config/redis.js"; // ✅ Added cacheKeys
import logger from "../config/logger.js";
import { prisma } from "./ledger.service.js";

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
    // ✅ REM:BL means a student topped up and was removed from the blacklist.
    // Invalidate their blacklist cache entry immediately — don't wait for the
    // 60s TTL to expire, otherwise their next tap would still be rejected.
    // Format of deltaCommand: "REM:BL,matricNumber"
    if (deltaCommand.startsWith("REM:BL,")) {
      const matricNumber = deltaCommand.split(",")[1];
      if (matricNumber) {
        await redis.del(cacheKeys.blacklist(matricNumber));
        log.debug(
          { matricNumber },
          "sync.blacklist_cache_invalidated_on_rem_bl"
        );
      }
    }

    const terminals = await prisma.terminal.findMany({
      select: { terminal_id: true, status: true },
    });

    if (terminals.length === 0) {
      log.warn("sync.fleet_broadcast_no_terminals");
      return;
    }

    log.info({ terminalCount: terminals.length }, "sync.fleet_broadcast_start");

    const promises = terminals.map(async (t) => {
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
    });

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

export {
  injectMqttPublisher,
  routeDeltaToTerminal,
  broadcastDeltaToFleet,
  flushTerminalQueue,
};
