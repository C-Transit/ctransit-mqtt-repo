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

// With enableOfflineQueue: false, any command issued before the stream is ready is
// rejected. Boot must therefore wait for "ready" instead of pinging immediately.
function waitForRedisReady(client: Redis, timeoutMs = 10000): Promise<void> {
  if (client.status === "ready") return Promise.resolve();

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      client.removeListener("ready", onReady);
      client.removeListener("error", onError);
    };
    const onReady = () => {
      cleanup();
      resolve();
    };
    const onError = (err: Error) => {
      cleanup();
      reject(err);
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Redis connection timed out"));
    }, timeoutMs);

    client.once("ready", onReady);
    client.once("error", onError);
  });
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

export { getRedisClient, waitForRedisReady, Redis, redisKeys, cacheKeys };