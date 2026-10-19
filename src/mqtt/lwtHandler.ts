// src/mqtt/lwtHandler.ts
import prisma from "../lib/prisma.js";
import { flushTerminalQueue } from "../services/sync.service.js";
import logger from "../config/logger.js";
import env from "../config/env.js";

type TerminalStatus = "ONLINE" | "OFFLINE" | "LOCKED";

interface TerminalStateCache {
  status: TerminalStatus;
  lastPersistedAt: number;
}

const VALID_STATUSES: TerminalStatus[] = ["ONLINE", "OFFLINE", "LOCKED"];
const STATUS_PERSIST_THROTTLE_MS = 30000; // 30 seconds
const MAX_CACHED_TERMINALS = 1000;

const terminalStatusCache = new Map<string, TerminalStateCache>();

async function handleLwtEvent(
  terminalId: string,
  statusPayload: string
): Promise<void> {
  const status = statusPayload.trim().toUpperCase() as TerminalStatus;
  const log = logger.child({ terminalId, status });

  if (!VALID_STATUSES.includes(status)) {
    log.warn(
      { payloadLength: statusPayload.length },
      "lwt.unknown_status_payload — ignoring"
    );
    return;
  }

  const now = Date.now();
  const cached = terminalStatusCache.get(terminalId);
  const statusChanged = !cached || cached.status !== status;
  const shouldPersist =
    statusChanged || now - cached.lastPersistedAt >= STATUS_PERSIST_THROTTLE_MS;

  try {
    if (shouldPersist) {
      if (env.terminals.autoRegister) {
        // Current behaviour: an unseen terminal gets a row automatically.
        const known = await prisma.terminal.findUnique({
          where: { terminal_id: terminalId },
          select: { terminal_id: true },
        });
        if (!known) {
          log.warn("lwt.terminal_auto_registered — check this is a real terminal");
        }

        await prisma.terminal.upsert({
          where: { terminal_id: terminalId },
          update: { status, last_seen: new Date() },
          create: {
            terminal_id: terminalId,
            status,
            last_seen: new Date(),
            secret_key: "UNPROVISIONED",
          },
        });
      } else {
        // Locked down: only terminals that already exist in the database count.
        const updated = await prisma.terminal.updateMany({
          where: { terminal_id: terminalId },
          data: { status, last_seen: new Date() },
        });
        if (updated.count === 0) {
          log.warn("lwt.unknown_terminal_ignored");
          return;
        }
      }

      if (terminalStatusCache.size >= MAX_CACHED_TERMINALS) {
        terminalStatusCache.clear();
      }
      terminalStatusCache.set(terminalId, { status, lastPersistedAt: now });

      log.info("lwt.terminal_status_updated");
    } else {
      log.debug("lwt.terminal_status_throttled");
    }

    if (status === "ONLINE") {
      log.info("lwt.terminal_online — flushing redis queue");
      await flushTerminalQueue(terminalId).catch((error: unknown) => {
        const errMessage =
          error instanceof Error ? error.message : "Unknown error";
        log.error({ err: errMessage }, "lwt.queue_flush_error");
      });
    } else {
      log.info("lwt.terminal_offline — status updated");
    }
  } catch (error) {
    const errMessage = error instanceof Error ? error.message : "Unknown error";
    log.error({ err: errMessage }, "lwt.db_update_error");
  }
}

export { handleLwtEvent };