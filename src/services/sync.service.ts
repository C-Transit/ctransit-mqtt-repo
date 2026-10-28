// src/services/sync.service.ts
"use strict";

import { randomBytes } from "node:crypto";
import { getRedisClient, redisKeys } from "../config/redis.js";
import logger from "../config/logger.js";
import prisma from "../lib/prisma.js";
import env from "../config/env.js";
import { buildChunks } from "../utils/parser.js";
import { publishToTerminal, commandTag } from "../mqtt/downlinkQueue.js";

// Only these commands may be wrapped for GSM delivery on enabled terminals.
const WRAP_PREFIXES = [
  "SYS:FARE,",
  "SYS:FARE_A,",
  "SYS:FARE_B,",
  "SYS:FARE_C,",
  "SYS:LOC,",
  "SYS:TIME,",
  "SYS:NET,",
  "ADD:WL,",
  "REM:WL,",
  "ADD:BL,",
  "REM:BL,",
];
const WRAP_EXACT = ["SYS:SYNC_COMPLETE"];
const WRAPPED = /^CMD:([0-9a-f]{12});([\s\S]*)$/;

function safeWorkerHost(): string | null {
  try {
    return env.worker.webhookUrl ? new URL(env.worker.webhookUrl).host : null;
  } catch {
    return env.worker.webhookUrl ? "invalid-url" : null;
  }
}

export function innerPayload(command: string): string {
  const m = WRAPPED.exec(command);
  return m ? m[2] : command;
}

export function buildCommand(terminalId: string, command: string): string {
  const normalizedTerminalId = terminalId.toUpperCase();

  if (WRAPPED.test(command)) return command;

  const eligible =
    WRAP_EXACT.includes(command) ||
    WRAP_PREFIXES.some((p) => command.startsWith(p));

  if (!eligible) return command;

  const gsmEnabled = env.cmdWrap.terminals.includes(normalizedTerminalId);
  if (!gsmEnabled) {
    logger.debug(
      {
        terminalId: normalizedTerminalId,
        command: commandTag(command),
        reason: "terminal_not_in_cmd_wrap_list",
        enabledTerminals: env.cmdWrap.terminals,
      },
      "sync.command_not_wrapped_for_gsm"
    );
    return command;
  }

  if (!env.worker.webhookUrl || !env.worker.webhookSecret) {
    throw new Error(
      `GSM wrap is enabled for terminal ${normalizedTerminalId} but WORKER_WEBHOOK_URL / WORKER_WEBHOOK_SECRET are missing.`
    );
  }

  const wrapped = `CMD:${randomBytes(6).toString("hex")};${command}`;
  logger.info(
    {
      terminalId: normalizedTerminalId,
      command: commandTag(command),
      wrappedCommand: commandTag(wrapped),
      workerHost: safeWorkerHost(),
    },
    "sync.command_wrapped_for_gsm"
  );
  return wrapped;
}

// Commands that describe a single current setting: only the newest one matters.
const STATE_PREFIXES = [
  "SYS:LOC,",
  "SYS:FARE,",
  "SYS:FARE_A,",
  "SYS:FARE_B,",
  "SYS:FARE_C,",
  "SYS:OTA,",
  "SYS:TIME,",
  "SYS:NET,",
];

// "ADD:BL,uid" / "REM:BL,uid" / "ADD:DR,uid,pin" -> "BL:uid" / "DR:uid".
// Two commands with the same key are about the same card, so the newest one wins.
const DELTA_PATTERN = /^(ADD|REM):([A-Z]+),([^,|]+)/;

function deltaKey(command: string): string | null {
  const match = DELTA_PATTERN.exec(command);
  return match ? `${match[2]}:${match[3]}` : null;
}

async function enqueueTerminalCommand(
  terminalId: string,
  command: string
): Promise<boolean> {
  const normId = terminalId.toUpperCase();
  const redis = getRedisClient();
  const queueKey = redisKeys.terminalQueue(normId);
  const tag = commandTag(command);

  try {
    const existing = await redis.lrange(queueKey, 0, -1);
    const multi = redis.multi();
    let replaced = 0;

    const inner = innerPayload(command);
    const matchingPrefix = STATE_PREFIXES.find((p) => inner.startsWith(p));
    const newKey = deltaKey(inner);

    if (matchingPrefix) {
      for (const item of existing) {
        if (innerPayload(item).startsWith(matchingPrefix)) {
          multi.lrem(queueKey, 0, item);
          replaced++;
        }
      }
    } else if (newKey) {
      // Last writer wins: drop older commands about the same card, then add the new one.
      for (const item of existing) {
        if (deltaKey(innerPayload(item)) === newKey) {
          multi.lrem(queueKey, 0, item);
          replaced++;
        }
      }
    } else if (existing.some((item) => innerPayload(item) === inner)) {
      logger.debug(
        { terminalId: normId, command: tag },
        "sync.enqueue_skipped_duplicate — command already queued"
      );
      return false;
    }

    multi.rpush(queueKey, command);
    const results = await multi.exec();
    const failed = results?.find(([err]) => err);
    if (!results || failed) {
      throw failed?.[0] ?? new Error("REDIS_MULTI_FAILED");
    }

    logger.info(
      { terminalId: normId, command: tag, replaced },
      "sync.command_queued_loss_resistant"
    );
    return true;
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    logger.error(
      { terminalId: normId, command: tag, err: errMsg },
      "sync.enqueue_command_error"
    );
    throw err;
  }
}

// Sends a command now if the terminal is online with nothing waiting, otherwise queues it.
// Throws only if the command could neither be sent nor saved.
async function routeDeltaToTerminal(
  terminalId: string,
  deltaCommand: string
): Promise<void> {
  const normId = terminalId.toUpperCase();
  const command = buildCommand(normId, deltaCommand);
  const redis = getRedisClient();
  const log = logger.child({
    terminalId: normId,
    command: commandTag(command),
  });
  const queueKey = redisKeys.terminalQueue(normId);

  try {
    const terminal = await prisma.terminal.findUnique({
      where: { terminal_id: normId },
      select: { status: true },
    });

    if (!terminal) {
      log.warn("sync.terminal_not_in_db — queuing delta");
      await enqueueTerminalCommand(normId, command);
      return;
    }

    const pendingCount = await redis.llen(queueKey);

    // Online and nothing waiting: send straight away.
    if (terminal.status === "ONLINE" && pendingCount === 0) {
      try {
        await publishToTerminal(normId, command);
        log.info("sync.delta_published_direct");
        return;
      } catch (pubErr) {
        const errMsg =
          pubErr instanceof Error ? pubErr.message : "Unknown error";
        log.warn(
          { err: errMsg },
          "sync.delta_direct_publish_failed — queuing command for retry"
        );
        await enqueueTerminalCommand(normId, command);
        return;
      }
    }

    // Offline, or older commands are waiting: queue it to keep the order.
    await enqueueTerminalCommand(normId, command);
    log.info(
      { terminalStatus: terminal.status, pendingCount },
      "sync.delta_queued_redis"
    );

    if (terminal.status === "ONLINE") {
      flushTerminalQueue(normId).catch((flushErr) => {
        log.error({ err: flushErr }, "sync.flush_queue_trigger_error");
      });
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    log.error({ err: errMsg }, "sync.route_delta_error — queuing as fallback");
    // If Redis is also down this throws, so the caller learns the command was
    // neither sent nor saved.
    await enqueueTerminalCommand(normId, command);
  }
}

export interface BroadcastSummary {
  totalTerminals: number;
  deliveredCount: number; // published directly
  queuedCount: number; // stored in Redis, delivered on reconnect
  failedTerminals: string[]; // neither published nor queued
}

async function broadcastDeltaToFleet(deltaCommand: string): Promise<BroadcastSummary> {
  const log = logger.child({ command: commandTag(deltaCommand) });
  const redis = getRedisClient();

  // SYSTEM_TERMINAL is a bookkeeping row for top-ups, not a physical device.
  const terminals = await prisma.terminal.findMany({
    where: { terminal_id: { not: "SYSTEM_TERMINAL" } },
    select: { terminal_id: true, status: true },
  });

  const summary: BroadcastSummary = {
    totalTerminals: terminals.length,
    deliveredCount: 0,
    queuedCount: 0,
    failedTerminals: [],
  };

  await Promise.all(
    terminals.map(async (t: { terminal_id: string; status: string }) => {
      const id = t.terminal_id.toUpperCase();
      const online = t.status === "ONLINE";
      const command = buildCommand(id, deltaCommand);

      try {
        const pending = online
          ? await redis.llen(redisKeys.terminalQueue(id)).catch(() => -1)
          : -1;

        if (online && pending === 0) {
          try {
            await publishToTerminal(id, command);
            summary.deliveredCount++;
            return;
          } catch (err) {
            log.warn(
              { terminalId: id, err: err instanceof Error ? err.message : String(err) },
              "sync.broadcast_direct_publish_failed — queuing to redis"
            );
          }
        }

        await enqueueTerminalCommand(id, command);
        summary.queuedCount++;

        if (online) {
          flushTerminalQueue(id).catch((err) =>
            log.error({ terminalId: id, err }, "sync.broadcast_flush_error")
          );
        }
      } catch (err) {
        log.error(
          { terminalId: id, err: err instanceof Error ? err.message : String(err) },
          "sync.broadcast_routing_failed"
        );
        summary.failedTerminals.push(id);
      }
    })
  );

  log.info(summary, "sync.fleet_broadcast_complete");
  return summary;
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
  const queueKey = redisKeys.terminalQueue(normId);

  try {
    const queueLength = await redis.llen(queueKey);
    if (queueLength === 0) {
      log.debug("sync.flush_queue_empty");
      return;
    }

    log.info({ queueLength }, "sync.flush_queue_start");

    let flushedCount = 0;

    // 1. Look at the first command without removing it.
    // 2. Send it.
    // 3. Only after the send succeeds, remove exactly that command.
    // If the send fails, the command stays in the queue for the next try.
    while (true) {
      const command = await redis.lindex(queueKey, 0);
      if (!command) break;

      try {
        await publishToTerminal(normId, command);

        // Remove this exact command, not "whatever is first now". If the command was
        // replaced while we were sending, this does nothing and the newer one stays.
        await redis.lrem(queueKey, 1, command);
        flushedCount++;
        log.debug(
          { command: commandTag(command), flushedCount },
          "sync.flush_queue_item_sent"
        );
      } catch (publishErr) {
        const errMsg =
          publishErr instanceof Error ? publishErr.message : "Unknown error";
        log.error(
          { command: commandTag(command), err: errMsg },
          "sync.flush_queue_publish_failed — command preserved in queue for retry"
        );
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

/// Send the full whitelist and blacklist to one terminal
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
    await publishToTerminal(
      terminalId,
      buildCommand(terminalId, "SYS:SYNC_COMPLETE")
    );
    log.info("sync.full_sync_complete");
  } catch (error) {
    const errMessage =
      error instanceof Error ? error.message : "Unknown error";
    log.error({ err: errMessage }, "sync.full_sync_error");
    throw error;
  }
}

export {
  enqueueTerminalCommand,
  routeDeltaToTerminal,
  broadcastDeltaToFleet,
  flushTerminalQueue,
  syncFullListsToTerminal,
};