import { Router } from "express";
import { validateApiKey } from "../middleware/api.middleware.js";
import {
  handleBroadcast,
  handleRoute,
  testBlacklistRemoval
} from "../controller/internal.controller.js";
import { syncFullListsToTerminal } from "../services/sync.service.js";

const router = Router();

// Enforce internal API key authentication on all internal endpoints
router.use(validateApiKey);

router.post("/broadcast", handleBroadcast);
router.post("/route/:terminalId", handleRoute);
router.post("/test-sync/:terminalId", async (req, res) => {
  const terminalId = Array.isArray(req.params.terminalId)
    ? req.params.terminalId[0]
    : req.params.terminalId;

  await syncFullListsToTerminal(terminalId);
  res.json({ success: true });
});
router.post("/test-remove-blacklist", testBlacklistRemoval);


export default router;
