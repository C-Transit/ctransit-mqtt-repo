import { prisma } from "../services/ledger.service.js";
import { flushTerminalQueue } from "../services/sync.service.js";
import logger from "../config/logger.js";

type TerminalStatus = "ONLINE" | "OFFLINE" | "LOCKED";

interface TerminalStateCache {
  status: TerminalStatus;
  lastPersistedAt: number;
}

const terminalStatusCache = new Map<string, TerminalStateCache>();
const STATUS_PERSIST_THROTTLE_MS = 30000; // 30 seconds

async function handleLwtEvent(
  terminalId: string,
  statusPayload: string
): Promise<void> {
  const status = statusPayload.trim().toUpperCase() as TerminalStatus;
  const log = logger.child({ terminalId, status });

  const VALID_STATUSES: TerminalStatus[] = ["ONLINE", "OFFLINE", "LOCKED"];
  if (!VALID_STATUSES.includes(status)) {
    log.warn(
      { rawPayload: statusPayload },
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
      await prisma.terminal.upsert({
        where: { terminal_id: terminalId },
        update: {
          status: status,
          last_seen: new Date(),
        },
        create: {
          terminal_id: terminalId,
          status: status,
          last_seen: new Date(),
          secret_key: "UNPROVISIONED",
        },
      });

      terminalStatusCache.set(terminalId, {
        status,
        lastPersistedAt: now,
      });

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

