// src/controllers/internal.controller.ts
import { Request, Response } from "express";
import {
  broadcastDeltaToFleet,
  routeDeltaToTerminal,
} from "../services/sync.service.js";
import logger from "../config/logger.js";
import prisma from "../lib/prisma.js";
import { buildDeltaCommand, normalizeDownlinkCommand } from '../utils/parser.js';

export const handleBroadcast = async (req: Request, res: Response) => {
  const { command: rawCommand } = req.body;
  if (!rawCommand || typeof rawCommand !== "string") {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }

  const command = normalizeDownlinkCommand(rawCommand);
  logger.debug({ command }, "internal.broadcast_request");
  await broadcastDeltaToFleet(command);
  res.json({ success: true, command });
};

export const handleRoute = async (req: Request, res: Response) => {
  const terminalIdRaw = req.params.terminalId;
  const terminalId = Array.isArray(terminalIdRaw)
    ? terminalIdRaw[0]
    : terminalIdRaw;
  const { command: rawCommand } = req.body;

  if (!rawCommand || typeof rawCommand !== "string") {
    return res
      .status(400)
      .json({ error: 'Missing or invalid "command" field' });
  }

  if (!terminalId) {
    return res.status(400).json({ error: "Missing terminalId in path" });
  }

  const command = normalizeDownlinkCommand(rawCommand);
  logger.debug({ terminalId, command }, "internal.route_request");
  await routeDeltaToTerminal(terminalId, command);
  res.json({ success: true, terminalId, command });
};

export const testBlacklistRemoval = async (req: Request, res: Response) => {
  const { studentUid } = req.body;

  if (!studentUid) {
    return res.status(400).json({ error: "Missing studentUid" });
  }

  // Get card UID
  const cardMap = await prisma.cardMapping.findUnique({
    where: { student_uid: studentUid },
    select: { card_uid: true },
  });

  if (!cardMap) {
    return res.status(404).json({ error: "No card linked to this student" });
  }

  // Broadcast REM:BL
  const command = buildDeltaCommand("REM", "BL", cardMap.card_uid);
  await broadcastDeltaToFleet(command);

  // Remove from DB
  await prisma.blacklist.deleteMany({
    where: { student_uid: studentUid },
  });

  logger.info(
    { studentUid, cardUid: cardMap.card_uid, command },
    "test.blacklist_removed"
  );

  res.json({
    success: true,
    cardUid: cardMap.card_uid,
    command,
  });
};