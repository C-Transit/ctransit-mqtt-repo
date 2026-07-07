// src/routes/health.routes.ts
import { Router, type Request, type Response } from "express";

const router = Router();

router.get("/", (_req: Request, res: Response) => {
  res.status(200).json({
    status: "ok",
    service: "ctransit-mqtt",
    timestamp: new Date().toISOString(),
  });
});

export default router;
