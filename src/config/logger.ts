// src/config/logger.ts
import pino, { type LoggerOptions } from "pino";
import env from "./env.js";

const nodeEnv = env.NODE_ENV || "development";
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
      "password",
      "*.password",
      "mqtt.password",
      "redis.password",
      "secret",
      "*.secret",
      "secret_key",
      "*.secret_key",
      "secretKey",
      "*.secretKey",
      "apiKey",
      "*.apiKey",
      "api_key",
      "*.api_key",
      "x-api-key",
      "*.x-api-key",
      "X-Api-Key",
      "*.X-Api-Key",
      "authorization",
      "*.authorization",
      "Authorization",
      "*.Authorization",
      "headers.authorization",
      "headers.Authorization",
      "headers['authorization']",
      "headers['Authorization']",
      "headers['x-api-key']",
      "headers['X-Api-Key']",
      "token",
      "*.token",
      "otp",
      "*.otp",
      "receivedSig",
      "*.receivedSig",
      "expectedSig",
      "*.expectedSig",
    ],
    censor: "[REDACTED]",
  },
};

const devOptions: LoggerOptions = {
  ...sharedBase,
  level: "debug",
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
      translateTime: "SYS:standard",
      ignore: "pid,hostname",
      singleLine: false,
    },
  },
};

const productionOptions: LoggerOptions = {
  ...sharedBase,
  level: "info",
};

const logger = pino(isDev ? devOptions : productionOptions);

export default logger;
