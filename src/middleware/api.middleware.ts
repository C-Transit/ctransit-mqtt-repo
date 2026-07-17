// src/middleware/apiKey.middleware.ts
import { Request, Response, NextFunction } from "express";
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

  if (!apiKey || apiKey !== expected) {
    logger.warn(
      { ip: req.ip, headers: req.headers },
      "internal.invalid_api_key"
    );
    return res.status(401).json({ error: "Invalid or missing API key" });
  }

  next();
};
