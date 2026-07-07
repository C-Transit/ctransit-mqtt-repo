import pino, { type LoggerOptions } from "pino";
import env from "./env.js";

const isDev = env.NODE_ENV === "development";

const loggerOptions: LoggerOptions = {
  level: isDev ? "debug" : "info",
  base: {
    service: "ctransit-backend-hardware-link",
    env: env.NODE_ENV,
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level(label) {
      return { level: label };
    },
  },
  redact: {
    paths: ["mqtt.password", "redis.password", "*.secret_key"],
    censor: "[REDACTED]",
  },
  // The modern TS-friendly way to configure Pino transports
  ...(isDev && {
    transport: {
      target: "pino-pretty",
      options: {
        colorize: true,
        translateTime: "SYS:standard",
        // Added 'env' to ignore list to clean up the console
        ignore: "pid,hostname,service,env",
        // Forces object payloads (like { terminalId: '...' }) to stay on one line
        singleLine: true,
      },
    },
  }),
};

// If not in dev, Pino defaults to standard JSON stdout (perfect for production)
const logger = pino(loggerOptions);

export default logger;
