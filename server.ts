"use strict";

import "./src/config/env.js";

import http from "http";
import express from "express";
import morgan from "morgan";
import { connectMqtt, disconnectMqtt } from "./src/mqtt/client.js";
import { getRedisClient } from "./src/config/redis.js";
import { prisma } from "./src/lib/prisma.js";
import healthRouter from "./src/routes/health.routes.js";
import internalRouter from "./src/routes/internal.routes.js";
import logger from "./src/config/logger.js";
import env from "./src/config/env.js";

const app = express();
app.set("trust proxy", 1);
const port = env.PORT;
const morganFormat = env.NODE_ENV === "production" ? "combined" : "dev";

app.use(morgan(morganFormat));
app.use(express.json());

// Health route
app.use("/health", healthRouter);

// Quick root route for Render health checks
app.get("/", (_req, res) => {
  res.status(200).json({ status: "ok", service: env.NODE_ENV === "production" ? "ctransit-mqtt" : "ctransit-mqtt-dev" });
});

app.use("/internal", internalRouter);

app.use((_req, res) => {
  res.status(404).json({ error: "Not found" });
});

const server = http.createServer(app);

let isShuttingDown = false;
let keepAliveInterval: NodeJS.Timeout | null = null;

async function boot(): Promise<void> {
  logger.info({ env: env.NODE_ENV }, "mqtt_service.boot_start");

  server.listen(port, () => {
    logger.info({ port }, "mqtt_service.http_listening");
  });

  try {
    const redis = getRedisClient();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (redis as any).ping();
    logger.info("mqtt_service.redis_ready");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    if (env.NODE_ENV === "production") {
      logger.fatal({ err: errMsg }, "mqtt_service.redis_failed — aborting");
      process.exit(1);
    } else {
      logger.warn({ err: errMsg }, "mqtt_service.redis_unavailable — continuing in dev mode");
    }
  }

  let pgConnected = false;
  try {
    await prisma.$connect();
    await prisma.$queryRaw`SELECT 1`;
    pgConnected = true;
    logger.info("mqtt_service.postgres_ready");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    if (env.NODE_ENV === "production") {
      logger.fatal({ err: errMsg }, "mqtt_service.postgres_failed — aborting");
      process.exit(1);
    } else {
      logger.warn({ err: errMsg }, "mqtt_service.postgres_unavailable — continuing in dev mode");
    }
  }

  try {
    await connectMqtt();
    logger.info("mqtt_service.mqtt_ready");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    if (env.NODE_ENV === "production") {
      logger.fatal({ err: errMsg }, "mqtt_service.mqtt_failed — aborting");
      process.exit(1);
    } else {
      logger.warn({ err: errMsg }, "mqtt_service.mqtt_unavailable — continuing in dev mode");
    }
  }

  // ── Start DB keep‑alive ping every 30 seconds only if connected ──
  if (pgConnected) {
    keepAliveInterval = setInterval(async () => {
      try {
        await prisma.$queryRaw`SELECT 1`;
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : "Unknown error";
        logger.warn({ err: errMsg }, "mqtt_service.db_keepalive_failed");
      }
    }, 30000);
  }

  logger.info("mqtt_service.boot_complete");
}

async function shutdown(signal: string, exitCode = 0): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info({ signal }, "mqtt_service.shutdown_initiated");

  // Clear keep‑alive interval
  if (keepAliveInterval) {
    clearInterval(keepAliveInterval);
    keepAliveInterval = null;
  }

  server.close();

  try {
    await disconnectMqtt();
    logger.info("mqtt_service.mqtt_disconnected");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    logger.error({ err: errMsg }, "mqtt_service.mqtt_disconnect_error");
  }

  try {
    const redis = getRedisClient();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (redis as any).quit();
    logger.info("mqtt_service.redis_disconnected");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    logger.error({ err: errMsg }, "mqtt_service.redis_disconnect_error");
  }

  try {
    await prisma.$disconnect();
    logger.info("mqtt_service.postgres_disconnected");
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    logger.error({ err: errMsg }, "mqtt_service.postgres_disconnect_error");
  }

  logger.info("mqtt_service.shutdown_complete");
  process.exit(exitCode);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

process.on("unhandledRejection", (reason: unknown) => {
  logger.error({ reason: String(reason) }, "mqtt_service.unhandled_rejection");
});

process.on("uncaughtException", (err: Error) => {
  logger.fatal({ err: err.message }, "mqtt_service.uncaught_exception");
  shutdown("uncaughtException", 1);
});

boot();
