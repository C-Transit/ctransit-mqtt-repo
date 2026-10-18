// src/controller/internal.controller.ts
import type { Request, Response } from "express";
import {
  broadcastDeltaToFleet,
  routeDeltaToTerminal,
} from "../services/sync.service.js";
import { commandTag } from "../mqtt/downlinkQueue.js";
import { normalizeDownlinkCommand } from "../utils/parser.js";
import logger from "../config/logger.js";

const MAX_COMMAND_LENGTH = 1000;
const TERMINAL_ID_PATTERN = /^[A-Za-z0-9_-]{1,20}$/;

const errText = (err: unknown): string =>
  err instanceof Error ? err.message : "Unknown error";

// Returns the cleaned command, or null if it is missing or unreasonable.
function readCommand(body: unknown): string | null {
  const raw = (body as { command?: unknown } | undefined)?.command;
  if (typeof raw !== "string") return null;
  const command = normalizeDownlinkCommand(raw);
  if (command.length === 0 || command.length > MAX_COMMAND_LENGTH) return null;
  return command;
}

export const handleBroadcast = async (req: Request, res: Response) => {
  const command = readCommand(req.body);
  if (!command) {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }

  // Commands can contain PINs, so only the type is ever logged or returned.
  const commandType = commandTag(command);

  try {
    logger.debug({ command: commandType }, "internal.broadcast_request");
    const summary = await broadcastDeltaToFleet(command);
    return res.json({ success: true, commandType, summary });
  } catch (err) {
    logger.error(
      { command: commandType, err: errText(err) },
      "internal.broadcast_failed"
    );
    return res.status(502).json({ success: false, error: "BROADCAST_FAILED" });
  }
};

export const handleRoute = async (req: Request, res: Response) => {
  const terminalIdRaw = req.params.terminalId;
  const terminalId = Array.isArray(terminalIdRaw)
    ? terminalIdRaw[0]
    : terminalIdRaw;

  if (!terminalId || !TERMINAL_ID_PATTERN.test(terminalId)) {
    return res.status(400).json({ error: "Missing or invalid terminalId in path" });
  }

  const command = readCommand(req.body);
  if (!command) {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }
  
  const commandType = commandTag(command);
  const normId = terminalId.toUpperCase();
  
  if (normId === "SYSTEM_TERMINAL") {
    return res.status(400).json({ error: "SYSTEM_TERMINAL is not a physical terminal" });
  }

  try {
    logger.debug(
      { terminalId: normId, command: commandType },
      "internal.route_request"
    );
    await routeDeltaToTerminal(normId, command);
    return res.json({ success: true, terminalId: normId, commandType });
  } catch (err) {
    logger.error(
      { terminalId: normId, command: commandType, err: errText(err) },
      "internal.route_failed"
    );
    return res.status(502).json({ success: false, error: "ROUTE_FAILED" });
  }
};