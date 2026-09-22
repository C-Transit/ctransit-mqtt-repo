// src/middleware/apiKey.middleware.ts
import { Request, Response, NextFunction } from "express";
import crypto from "crypto";
import env from "../config/env.js";
import logger from "../config/logger.js";

export const validateApiKey = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const apiKeyRaw = req.headers["x-api-key"];
  const apiKey = Array.isArray(apiKeyRaw) ? apiKeyRaw[0] : apiKeyRaw;
  const expected = env.mqtt.internalSecret;

  if (!apiKey || !expected) {
    logger.warn(
      { ip: req.ip, path: req.originalUrl || req.url },
      "internal.invalid_or_missing_api_key"
    );
    return res.status(401).json({ error: "Invalid or missing API key" });
  }

  const apiKeyBuffer = Buffer.from(apiKey);
  const expectedBuffer = Buffer.from(expected);

  const isValid =
    apiKeyBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(apiKeyBuffer, expectedBuffer);

  if (!isValid) {
    logger.warn(
      { ip: req.ip, path: req.originalUrl || req.url },
      "internal.invalid_api_key"
    );
    return res.status(401).json({ error: "Invalid or missing API key" });
  }

  next();
};
