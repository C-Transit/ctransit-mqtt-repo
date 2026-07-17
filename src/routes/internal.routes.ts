import { Router } from "express";
import { validateApiKey } from "../middleware/api.middleware.js";
import {
  handleBroadcast,
  handleRoute,
} from "../controller/internal.controller.js";

const router = Router();

router.post("/broadcast", validateApiKey, handleBroadcast);
router.post("/route/:terminalId", validateApiKey, handleRoute);


export default router;
