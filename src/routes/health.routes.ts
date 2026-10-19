// src/routes/health.routes.ts
import { Router, type Request, type Response } from "express";
import prisma from "../lib/prisma.js";
import logger from "../config/logger.js";

const router = Router();

router.get("/", async (_req: Request, res: Response) => {
  try {
    await prisma.$queryRaw`SELECT 1`;
    res.status(200).json({
      status: "ok",
      service: "ctransit-mqtt",
      db: "connected",
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : "Unknown error";
    logger.warn({ err: errMsg }, "health.db_check_failed");
    res.status(503).json({
      status: "unhealthy",
      service: "ctransit-mqtt",
      db: "disconnected",
      timestamp: new Date().toISOString(),
    });
  }
});

export default router;