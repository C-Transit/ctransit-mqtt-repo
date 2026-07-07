import { prisma } from "../services/ledger.service.js";
import { flushTerminalQueue } from "../services/sync.service.js";
import logger from "../config/logger.js";
import type { TerminalStatus } from "@prisma/client";

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

  try {
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

    log.info("lwt.terminal_status_updated");

    if (status === "ONLINE") {
      log.info("lwt.terminal_online — flushing redis queue");
      flushTerminalQueue(terminalId).catch((error: unknown) => {
        const errMessage =
          error instanceof Error ? error.message : "Unknown error";
        log.error({ err: errMessage }, "lwt.queue_flush_error");
      });
    } else {
      log.info("lwt.terminal_offline — status locked in db");
    }
  } catch (error) {
    const errMessage = error instanceof Error ? error.message : "Unknown error";
    log.error({ err: errMessage }, "lwt.db_update_error");
  }
}

export { handleLwtEvent };
