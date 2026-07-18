import { Router } from "express";
import { validateApiKey } from "../middleware/api.middleware.js";
import {
  handleBroadcast,
  handleRoute,
  testDeduct,
} from "../controller/internal.controller.js";
import { syncFullListsToTerminal } from "../services/sync.service.js";

const router = Router();

router.post("/broadcast", validateApiKey, handleBroadcast);
router.post("/route/:terminalId", validateApiKey, handleRoute);
router.post("/test-sync/:terminalId", validateApiKey, async (req, res) => {
  const terminalId = Array.isArray(req.params.terminalId)
    ? req.params.terminalId[0]
    : req.params.terminalId;

  await syncFullListsToTerminal(terminalId);
  res.json({ success: true });
});
router.post("/test-deduct", testDeduct);

export default router;
