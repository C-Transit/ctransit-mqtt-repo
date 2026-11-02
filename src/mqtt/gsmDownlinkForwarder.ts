import { randomUUID } from "node:crypto";
import env from "../config/env.js";
import logger from "../config/logger.js";
import { getRedisClient, redisKeys } from "../config/redis.js";

const POST_TIMEOUT_MS = 5000;
const RETRY_INTERVAL_MS = 5000;
const LOCK_TTL_MS = 15000;
const CMD_ID_PATTERN = /^CMD:([0-9a-f]{12});/;

interface GsmOutboxItem {
  deliveryId: string;
  terminalId: string;
  commandCsv: string;
}

function commandTag(command: string): string {
  return command.replace(CMD_ID_PATTERN, "").split(",")[0];
}

function workerHost(): string | null {
  try {
    return env.worker.webhookUrl ? new URL(env.worker.webhookUrl).host : null;
  } catch {
    return env.worker.webhookUrl ? "invalid-url" : null;
  }
}

async function postToWorker(item: GsmOutboxItem): Promise<boolean> {
  const webhookUrl = env.worker.webhookUrl?.trim();
  const webhookSecret = env.worker.webhookSecret?.trim();

  if (!webhookUrl || !webhookSecret) {
    logger.error({
      terminalId: item.terminalId,
      deliveryId: item.deliveryId,
      missing: [
        ...(!webhookUrl ? ["WORKER_WEBHOOK_URL"] : []),
        ...(!webhookSecret ? ["WORKER_WEBHOOK_SECRET"] : []),
      ],
    }, "gsm_downlink.worker_configuration_missing");
    return false;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), POST_TIMEOUT_MS);
  try {
    const res = await fetch(webhookUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Worker-Secret": webhookSecret,
      },
      body: JSON.stringify({
        validatorId: item.terminalId,
        commandCsv: item.commandCsv,
        deliveryId: item.deliveryId,
      }),
      signal: controller.signal,
    });
    const body = await res.text();
    let response: { puback?: unknown; deliveryId?: unknown } | null = null;
    try {
      response = JSON.parse(body) as { puback?: unknown; deliveryId?: unknown };
    } catch {
      response = null;
    }

    const acknowledged =
      res.ok &&
      response?.puback === true &&
      response.deliveryId === item.deliveryId;
    if (acknowledged) {
      logger.info({
        terminalId: item.terminalId,
        deliveryId: item.deliveryId,
        command: commandTag(item.commandCsv),
        workerHost: workerHost(),
      }, "gsm_downlink.puback_received");
      return true;
    }

    logger.warn({
      terminalId: item.terminalId,
      deliveryId: item.deliveryId,
      command: commandTag(item.commandCsv),
      workerHost: workerHost(),
      status: res.status,
      pubackReceived: response?.puback === true,
      acknowledgementIdMatches: response?.deliveryId === item.deliveryId,
    }, "gsm_downlink.puback_missing_retrying");
    return false;
  } catch (err) {
    logger.warn({
      terminalId: item.terminalId,
      deliveryId: item.deliveryId,
      command: commandTag(item.commandCsv),
      workerHost: workerHost(),
      reason:
        err instanceof Error && err.name === "AbortError"
          ? "timeout"
          : err instanceof Error
            ? err.message
            : "unknown",
    }, "gsm_downlink.worker_request_failed_retrying");
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function enqueueGsmDownlink(
  terminalId: string,
  commandCsv: string
): Promise<void> {
  const item: GsmOutboxItem = {
    terminalId: terminalId.toUpperCase(),
    commandCsv,
    deliveryId: `${terminalId.toUpperCase()}:${
      CMD_ID_PATTERN.exec(commandCsv)?.[1] ?? randomUUID()
    }`,
  };
  const redis = getRedisClient();
  await redis.rpush(redisKeys.gsmOutbox, JSON.stringify(item));
  logger.info({
    terminalId: item.terminalId,
    deliveryId: item.deliveryId,
    command: commandTag(commandCsv),
  }, "gsm_downlink.queued_for_worker");
  void flushGsmOutbox().catch((err: unknown) => {
    logger.error({
      err: err instanceof Error ? err.message : "Unknown error",
    }, "gsm_downlink.outbox_trigger_failed");
  });
}

async function migrateQueuedGsmCommands(): Promise<void> {
  const redis = getRedisClient();

  for (const terminalId of env.cmdWrap.terminals) {
    const queueKey = redisKeys.terminalQueue(terminalId);
    const queuedCommands = await redis.lrange(queueKey, 0, -1);
    if (queuedCommands.length === 0) continue;

    const migrated = queuedCommands.map((commandCsv): GsmOutboxItem => ({
      terminalId,
      commandCsv,
      deliveryId: `${terminalId}:${
        CMD_ID_PATTERN.exec(commandCsv)?.[1] ?? randomUUID()
      }`,
    }));
    const multi = redis.multi();
    multi.rpush(
      redisKeys.gsmOutbox,
      ...migrated.map((item) => JSON.stringify(item))
    );
    multi.del(queueKey);
    const results = await multi.exec();
    const failed = results?.find(([err]) => err);
    if (!results || failed) {
      throw failed?.[0] ?? new Error("GSM_QUEUE_MIGRATION_FAILED");
    }

    logger.info({
      terminalId,
      migratedCount: migrated.length,
    }, "gsm_downlink.legacy_queue_migrated");
  }
}

let draining = false;
let retryTimer: NodeJS.Timeout | null = null;

async function flushGsmOutbox(): Promise<void> {
  if (draining) return;
  draining = true;
  const redis = getRedisClient();
  const lockToken = randomUUID();
  let lockAcquired = false;
  let shouldContinue = false;

  try {
    lockAcquired =
      (await redis.set(
        redisKeys.gsmOutboxLock,
        lockToken,
        "PX",
        LOCK_TTL_MS,
        "NX"
      )) === "OK";
    if (!lockAcquired) return;

    const queued = await redis.lindex(redisKeys.gsmOutbox, 0);
    if (!queued) return;

    let item: GsmOutboxItem;
    try {
      item = JSON.parse(queued) as GsmOutboxItem;
      if (
        !item.deliveryId ||
        !item.terminalId ||
        typeof item.commandCsv !== "string"
      ) {
        throw new Error("Invalid GSM outbox item");
      }
    } catch (err) {
      logger.error({
        err: err instanceof Error ? err.message : "Unknown error",
      }, "gsm_downlink.invalid_outbox_item");
      return;
    }

    const acknowledged = await postToWorker(item);
    if (acknowledged) {
      const removed = await redis.lrem(redisKeys.gsmOutbox, 1, queued);
      if (removed !== 1) {
        logger.error({
          terminalId: item.terminalId,
          deliveryId: item.deliveryId,
          removed,
        }, "gsm_downlink.acknowledged_item_not_removed");
      } else {
        shouldContinue = true;
      }
    }
  } catch (err) {
    logger.error({
      err: err instanceof Error ? err.message : "Unknown error",
    }, "gsm_downlink.outbox_processing_failed");
  } finally {
    if (lockAcquired) {
      await redis.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        1,
        redisKeys.gsmOutboxLock,
        lockToken
      ).catch((err: unknown) => {
        logger.error({
          err: err instanceof Error ? err.message : "Unknown error",
        }, "gsm_downlink.outbox_lock_release_failed");
      });
    }
    draining = false;
    if (shouldContinue) {
      void flushGsmOutbox().catch((err: unknown) => {
        logger.error({
          err: err instanceof Error ? err.message : "Unknown error",
        }, "gsm_downlink.outbox_trigger_failed");
      });
    }
  }
}

async function startGsmDownlinkForwarder(): Promise<void> {
  await migrateQueuedGsmCommands();
  await flushGsmOutbox();
  if (retryTimer) clearInterval(retryTimer);
  retryTimer = setInterval(() => {
    void flushGsmOutbox().catch((err: unknown) => {
      logger.error({
        err: err instanceof Error ? err.message : "Unknown error",
      }, "gsm_downlink.outbox_retry_failed");
    });
  }, RETRY_INTERVAL_MS);
  logger.info({
    workerConfigured: Boolean(env.worker.webhookUrl && env.worker.webhookSecret),
    workerHost: workerHost(),
    intervalMs: RETRY_INTERVAL_MS,
  }, "gsm_downlink.outbox_started");
}

function stopGsmDownlinkForwarder(): void {
  if (retryTimer) clearInterval(retryTimer);
  retryTimer = null;
}

export {
  enqueueGsmDownlink,
  flushGsmOutbox,
  startGsmDownlinkForwarder,
  stopGsmDownlinkForwarder,
};
