// src/controllers/internal.controller.ts
import { Request, Response } from "express";
import {
  broadcastDeltaToFleet,
  routeDeltaToTerminal,
} from "../services/sync.service.js";
import logger from "../config/logger.js";
// src/controllers/internal.controller.ts – add this function

import { deductFare, isBelowThreshold, prisma } from '../services/ledger.service.js';
import { buildDeltaCommand } from '../utils/parser.js';

export const testDeduct = async (req: Request, res: Response) => {
  const { studentUid, amount, terminalId } = req.body;

  if (!studentUid || !amount || !terminalId) {
    return res.status(400).json({ 
      error: 'Missing fields: studentUid, amount, terminalId' 
    });
  }

  // 1. Get the card UID for this student
  const cardMap = await prisma.cardMapping.findUnique({
    where: { student_uid: studentUid },
    select: { card_uid: true }
  });
  if (!cardMap) {
    return res.status(404).json({ error: 'No card linked to this student' });
  }
  const cardUid = cardMap.card_uid;

  // 2. Perform deduction (this updates balance)
  const result = await deductFare(
    studentUid,
    amount,
    `TEST-${Date.now()}`,
    prisma
  );

  if (!result.walletFound) {
    return res.status(404).json({ error: 'Wallet not found' });
  }

  const newBalance = result.newBalance!;
  const log = logger.child({ studentUid, cardUid, newBalance, terminalId });

  // 3. Check if below threshold
  if (isBelowThreshold(newBalance)) {
    log.info('test.balance_below_threshold — blacklisting');
    // Add to blacklist DB
    await prisma.blacklist.upsert({
      where: { student_uid: studentUid },
      update: { blacklistedAt: new Date() },
      create: { student_uid: studentUid, reason: 'LOW_BALANCE' },
    });
    // Broadcast to all terminals
    const command = buildDeltaCommand('ADD', 'BL', cardUid);
    await broadcastDeltaToFleet(command);
    log.info({ command }, 'test.blacklist_broadcast_sent');
    return res.json({
      success: true,
      newBalance,
      blacklisted: true,
      command,
    });
  } else {
    log.info('test.balance_above_threshold — no blacklist');
    return res.json({
      success: true,
      newBalance,
      blacklisted: false,
    });
  }
};

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