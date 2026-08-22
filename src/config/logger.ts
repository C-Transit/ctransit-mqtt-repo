// src/config/logger.ts
import pino, { type LoggerOptions } from "pino";

const nodeEnv = process.env.NODE_ENV || "development";
const isDev = nodeEnv !== "production";

const sharedBase = {
  base: {
    service: "ctransit-mqtt",
    env: nodeEnv,
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level(label: string) {
      return { level: label };
    },
  },
  redact: {
    paths: [
      "mqtt.password",
      "redis.password",
      "*.secret_key",
      "password",
      "*.password",
    ],
    censor: "[REDACTED]",
  },
};

const devOptions: LoggerOptions = {
  ...sharedBase,
  level: "debug",
};

const productionOptions: LoggerOptions = {
  ...sharedBase,
  level: "info",
};

const logger = pino(isDev ? devOptions : productionOptions);

export default logger;
