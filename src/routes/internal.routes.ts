import { Router } from "express";
import { validateApiKey } from "../middleware/api.middleware.js";
import {
  handleBroadcast,
  handleRoute,
} from "../controller/internal.controller.js";

const router = Router();

router.post("/broadcast", validateApiKey, handleBroadcast);
router.post("/route/:terminalId", validateApiKey, handleRoute);
router.get('/debug-headers', (req, res) => {
  res.json({
    headers: req.headers,
    apiKeyFromEnv: process.env.MQTT_INTERNAL_SECRET,
  });
});

export default router;
