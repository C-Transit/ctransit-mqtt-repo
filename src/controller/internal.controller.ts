// src/controllers/internal.controller.ts
import { Request, Response } from "express";
import {
  broadcastDeltaToFleet,
  routeDeltaToTerminal,
} from "../services/sync.service.js";
import logger from "../config/logger.js";

export const handleBroadcast = async (req: Request, res: Response) => {
  const { command } = req.body;
  if (!command || typeof command !== "string") {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }

  logger.debug({ command }, "internal.broadcast_request");
  await broadcastDeltaToFleet(command);
  res.json({ success: true, command });
};

export const handleRoute = async (req: Request, res: Response) => {
  const terminalIdRaw = req.params.terminalId;
  const terminalId = Array.isArray(terminalIdRaw)
    ? terminalIdRaw[0]
    : terminalIdRaw;
  const { command } = req.body;

  if (!command || typeof command !== "string") {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }

  if (!terminalId) {
    return res.status(400).json({ error: "Missing terminalId in path" });
  }

  logger.debug({ terminalId, command }, "internal.route_request");
  await routeDeltaToTerminal(terminalId, command);
  res.json({ success: true, terminalId, command });
};
