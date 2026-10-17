// src/config/redis.ts
import Redis from "ioredis";
import env from "./env.js";
import logger from "./logger.js";

let redisClient: Redis | null = null;

// Host only. The full URL contains the password and must never be logged.
function redisHost(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return "invalid-url";
  }
}

function getRedisClient(): Redis {
  if (redisClient) return redisClient;

  const redisUrl = env.redis.url;

  logger.info({ redisHost: redisHost(redisUrl) }, "redis.connecting");
  redisClient = new Redis(redisUrl, {
    lazyConnect: false,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 3,
  });

  redisClient.on("error", (error) => {
    logger.error({ err: error }, "redis.connection_error");
  });

  return redisClient;
}

// Per-terminal downlink queue: RPUSH queue:8hy1k "ADD:WL,uid"
const redisKeys = {
  terminalQueue: (terminalId: string): string =>
    `queue:${terminalId.toLowerCase()}`,
};

const cacheKeys = {
  // Hardware card UID -> student matricNumber (invalidated on card re-link)
  cardMap: (cardUid: string): string => `card:map:${cardUid}`,
};

export { getRedisClient, Redis, redisKeys, cacheKeys };