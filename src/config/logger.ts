// src/config/logger.ts
import pino, { type LoggerOptions } from "pino";
import env from "./env.js";

const isDev = env.NODE_ENV !== "production";

const sharedBase = {
  base: {
    service: "ctransit-mqtt",
    env: env.NODE_ENV,
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
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
      translateTime: "HH:MM:ss",
      ignore: "pid,hostname,service,env",
      singleLine: true,
      levelFirst: false,
      messageFormat:
        "{msg}" +
        "{if terminalId}  \u001b[36m[{terminalId}]\u001b[39m{end}" +
        "{if topic}  \u001b[2mtopic={topic}\u001b[22m{end}" +
        "{if command}  \u001b[33mcmd={command}\u001b[39m{end}" +
        "{if payloadType}  \u001b[35mtype={payloadType}\u001b[39m{end}" +
        "{if rawPayload}  \u001b[2mpayload={rawPayload}\u001b[22m{end}" +
        "{if matricNumber}  \u001b[32mmatric={matricNumber}\u001b[39m{end}" +
        "{if previousBalance}  \u001b[2mprev=\u20a6{previousBalance}\u001b[22m{end}" +
        "{if newBalance}  \u001b[32mnew=\u20a6{newBalance}\u001b[39m{end}" +
        "{if status}  \u001b[33mstatus={status}\u001b[39m{end}" +
        "{if driverUid}  \u001b[36mdriver={driverUid}\u001b[39m{end}" +
        "{if cardUid}  \u001b[36mcard={cardUid}\u001b[39m{end}" +
        "{if otp}  \u001b[33motp={otp}\u001b[39m{end}" +
        "{if agentId}  \u001b[36magent={agentId}\u001b[39m{end}" +
        "{if userId}  \u001b[2muser={userId}\u001b[22m{end}" +
        "{if tokenId}  \u001b[2mtoken={tokenId}\u001b[22m{end}" +
        "{if err}  \u001b[31merr={err}\u001b[39m{end}",
    },
  },
};

const productionOptions: LoggerOptions = {
  ...sharedBase,
  level: "info",
};

const logger = pino(isDev ? devOptions : productionOptions);

export default logger;
