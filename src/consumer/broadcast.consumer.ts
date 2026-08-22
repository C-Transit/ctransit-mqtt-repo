// src/consumer/broadcast.consumer.ts
//
// DEPRECATED & INACTIVATED:
// As per the Performance & Cost Audit, the duplicate 500ms polling loop
// and `KEYS mqtt:route:*` pattern have been removed.
//
// The canonical architecture uses authenticated internal HTTP calls:
// Main API -> POST /internal/broadcast or POST /internal/route/:terminalId
// -> sync.service -> MQTT downlink / Redis durable offline queue.
//
// This eliminates ~345,600 idle Redis commands/day and KEYS scans.

import logger from "../config/logger.js";

export async function startBroadcastConsumer(): Promise<void> {
  logger.info(
    "broadcast_consumer.inactive — using canonical internal HTTP route endpoints"
  );
}

export function stopBroadcastConsumer(): void {
  // No-op
}

