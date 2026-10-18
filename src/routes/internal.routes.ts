// src/routes/internal.routes.ts
import { Router } from "express";
import { validateApiKey } from "../middleware/api.middleware.js";
import {
  handleBroadcast,
  handleRoute,
} from "../controller/internal.controller.js";

const router = Router();

// Every internal endpoint requires the internal API key.
router.use(validateApiKey);

router.post("/broadcast", handleBroadcast);
router.post("/route/:terminalId", handleRoute);

export default router;