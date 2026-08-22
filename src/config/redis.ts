// src/config/redis.ts
import { Redis, type RedisOptions } from "ioredis";
import env from "./env.js";
import logger from "./logger.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let redisClient: any = null;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getRedisClient(): any {
  if (redisClient) return redisClient;

  const options: RedisOptions = {
    db: 0,
    retryStrategy(times: number): number | null {
      if (times > 5) {
        logger.warn({ times }, "redis.max_retries_reached — pausing reconnection");
        return null;
      }
      const delay = Math.min(times * 500, 3000);
      return delay;
    },
    enableOfflineQueue: true,
    maxRetriesPerRequest: null,
    lazyConnect: true,
  };

  redisClient = new Redis(env.redis.url, options);

  redisClient.on("connect", () =>
    logger.info("redis.connected")
  );
  redisClient.on("ready", () =>
    logger.info("redis.ready")
  );
  redisClient.on("error", (err: Error) =>
    logger.warn({ err: err.message }, "redis.warning")
  );
  redisClient.on("close", () =>
    logger.warn("redis.connection_closed")
  );

  return redisClient;
}

// ── MQTT Bridge & Terminal Queue Keys ─────────────────────────────────────────
// These are the ONLY Redis keys used in the MQTT service.
// Wallet, blacklist, agent status, OTP, and refresh token
// caches have been removed — now handled by direct DB queries.
const redisKeys = {
  // Per-terminal downlink queue: RPUSH queue:term_01 "ADD:WL,uid"
  terminalQueue: (terminalId: string): string =>
    `queue:${terminalId.toLowerCase()}`,
};

const cacheKeys = {
  // Maps hardware card UID → student matricNumber
  // Cached with a 7-day TTL; invalidated immediately on card re-link
  // This is the ONLY cache remaining in the MQTT service
  cardMap: (cardUid: string): string => `card:map:${cardUid}`,
};

export { getRedisClient, redisKeys, cacheKeys };
