// src/config/redis.ts
import Redis from "ioredis";
import env from "./env.js";
import logger from "./logger.js";

let redisClient: Redis | null = null;

function getRedisClient(): Redis {
  if (redisClient) return redisClient;

  const redisUrl = env.redis.url;

  logger.info({ redisUrl }, "redis.connecting");
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

function setRedisClient(client: Redis): void {
  redisClient = client;
}

async function waitForRedisReady(client: Redis): Promise<void> {
  if (client.status === "ready") return;

  await new Promise<void>((resolve, reject) => {
    const onReady = () => {
      cleanup();
      resolve();
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      client.off("ready", onReady);
      client.off("error", onError);
    };

    client.once("ready", onReady);
    client.once("error", onError);
  });
}

// MQTT Bridge & Terminal Queue Keys
// These are the ONLY Redis keys used in the MQTT service.
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

export {
  getRedisClient,
  setRedisClient,
  waitForRedisReady,
  Redis,
  redisKeys,
  cacheKeys,
};

