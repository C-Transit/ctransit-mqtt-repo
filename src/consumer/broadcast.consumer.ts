// src/consumer/broadcast.consumer.ts
//
// Polls the shared Redis broadcast queue written to by the main API.
// When the main API needs to broadcast a delta to terminals
// (e.g. after KYC approval, wallet top-up, or blacklist update),
// it pushes a command to "mqtt:broadcast" instead of calling
// broadcastDeltaToFleet() directly — since it no longer has
// an MQTT connection.
//
// This consumer reads from that queue and executes the broadcast.
// It also handles targeted single-terminal commands via
// "mqtt:route:{terminalId}" queues.
//
// Queue formats:
//   mqtt:broadcast       → any broadcastDeltaToFleet() command
//                          e.g. "ADD:WL,238DB4E8" / "REM:BL,2022/1/87453LH"
//   mqtt:route:{id}      → targeted routeDeltaToTerminal() command
//                          e.g. key="mqtt:route:TERM-01" value="ADD:WL,238DB4E8"

import { getRedisClient } from "../config/redis.js";
import {
  broadcastDeltaToFleet,
  routeDeltaToTerminal,
} from "../services/sync.service.js";
import logger from "../config/logger.js";

const BROADCAST_QUEUE = "mqtt:broadcast";
const ROUTE_QUEUE_PREFIX = "mqtt:route:";
const POLL_INTERVAL_MS = 500; // Poll every 500ms — fast enough for real-time feel

let isRunning = false;

// ─────────────────────────────────────────────
// startBroadcastConsumer
// Starts two polling loops:
//   1. Broadcast loop — drains mqtt:broadcast queue
//   2. Route loop — scans mqtt:route:* keys and drains each
//
// Both run on the same interval. Uses lpop (non-blocking)
// rather than blpop (blocking) because ioredis on Upstash
// serverless has connection timeout issues with blocking
// commands held open for long periods.
// ─────────────────────────────────────────────
export async function startBroadcastConsumer(): Promise<void> {
  if (isRunning) return;
  isRunning = true;

  logger.info("broadcast_consumer.started");

  const redis = getRedisClient();

  const poll = async () => {
    try {
      // ── Drain broadcast queue ─────────────────────────────────────
      let command: string | null;
      while ((command = await redis.lpop(BROADCAST_QUEUE)) !== null) {
        const log = logger.child({ command });
        try {
          await broadcastDeltaToFleet(command);
          log.info("broadcast_consumer.broadcast_executed");
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : String(err);
          log.error(
            { err: errMsg },
            "broadcast_consumer.broadcast_failed — re-queuing"
          );
          // Re-queue at front so it's retried next poll
          await redis.lpush(BROADCAST_QUEUE, command);
          break; // Stop draining — don't process further until this one succeeds
        }
      }

      // ── Drain targeted route queues ───────────────────────────────
      // Scan for all mqtt:route:* keys — each is a per-terminal queue
      const routeKeys = await redis.keys(`${ROUTE_QUEUE_PREFIX}*`);
      for (const key of routeKeys) {
        const terminalId = key.replace(ROUTE_QUEUE_PREFIX, "").toUpperCase();
        let routeCmd: string | null;
        while ((routeCmd = await redis.lpop(key)) !== null) {
          const rLog = logger.child({ terminalId, command: routeCmd });
          try {
            await routeDeltaToTerminal(terminalId, routeCmd);
            rLog.info("broadcast_consumer.route_executed");
          } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            rLog.error(
              { err: errMsg },
              "broadcast_consumer.route_failed — re-queuing"
            );
            await redis.lpush(key, routeCmd);
            break;
          }
        }
      }
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      logger.error({ err: errMsg }, "broadcast_consumer.poll_error");
    }

    // Schedule next poll
    setTimeout(poll, POLL_INTERVAL_MS);
  };

  // Start polling
  setTimeout(poll, POLL_INTERVAL_MS);
}

export function stopBroadcastConsumer(): void {
  isRunning = false;
  logger.info("broadcast_consumer.stopped");
}
