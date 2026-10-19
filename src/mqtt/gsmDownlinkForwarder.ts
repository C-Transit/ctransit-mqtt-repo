import type { MqttClient } from "mqtt";
import env from "../config/env.js";
import logger from "../config/logger.js";

const POST_TIMEOUT_MS = 5000;
const MAX_ATTEMPTS = 3;
const BACKOFF_BASE_MS = 500;
const CMD_PATTERN = /^CMD:([0-9a-f]{12});/;

type PostResult = { done: boolean; retry: boolean; reason: string };

function terminalIdFromTopic(topic: string): string | null {
  const parts = topic.split("/");
  if (parts.length !== 3 || parts[0] !== "ctransit" || parts[2] !== "rx") return null;
  const id = parts[1].toUpperCase();
  return id === "SERVER" ? null : id;
}

async function postToWorker(terminalId: string, commandCsv: string): Promise<PostResult> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), POST_TIMEOUT_MS);
  try {
    const res = await fetch(env.worker.webhookUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Worker-Secret": env.worker.webhookSecret,
      },
      body: JSON.stringify({ validatorId: terminalId, commandCsv }),
      signal: controller.signal,
    });
    const body = await res.text().catch(() => "");
    if (res.ok || body.includes("ALREADY_QUEUED")) {
      return { done: true, retry: false, reason: "ok" };
    }
    if (res.status >= 400 && res.status < 500) {
      return { done: false, retry: false, reason: `http_${res.status}` };
    }
    return { done: false, retry: true, reason: `http_${res.status}` };
  } catch (err) {
    const aborted = err instanceof Error && err.name === "AbortError";
    return { done: false, retry: true, reason: aborted ? "timeout" : "network" };
  } finally {
    clearTimeout(timer);
  }
}

async function forwardWithRetry(
  terminalId: string,
  commandCsv: string,
  log: ReturnType<typeof logger.child>
): Promise<void> {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const r = await postToWorker(terminalId, commandCsv);
    if (r.done) {
      log.info({ attempt }, "gsm_downlink.forwarded");
      return;
    }
    if (!r.retry) {
      log.error({ attempt, reason: r.reason }, "gsm_downlink.rejected_by_worker");
      return;
    }
    if (attempt === MAX_ATTEMPTS) break;
    const backoffMs = BACKOFF_BASE_MS * 2 ** (attempt - 1);
    log.warn({ attempt, reason: r.reason, backoffMs }, "gsm_downlink.retry_scheduled");
    await new Promise((resolve) => setTimeout(resolve, backoffMs));
  }
  log.error({ attempts: MAX_ATTEMPTS }, "gsm_downlink.forward_failed_permanently");
}

// One command at a time per terminal, so ADD:BL then REM:BL never swap order.
const chains = new Map<string, Promise<void>>();
function runInOrder(terminalId: string, job: () => Promise<void>): void {
  const prev = chains.get(terminalId) ?? Promise.resolve();
  const next = prev.then(job).catch(() => undefined);
  chains.set(terminalId, next);
  void next.then(() => {
    if (chains.get(terminalId) === next) chains.delete(terminalId);
  });
}

export function attachGsmDownlinkForwarder(client: MqttClient): void {
  client.on("message", (topic, payload, packet) => {
    const terminalId = terminalIdFromTopic(topic);
    if (!terminalId) return;
    if (packet.retain) return;
    const commandCsv = payload.toString("utf8").trim();
    const match = CMD_PATTERN.exec(commandCsv);
    if (!match) return; // not a wrapped fleet command
    const log = logger.child({
      terminalId,
      commandId: match[1],
      commandLength: commandCsv.length,
    });
    log.info("gsm_downlink.received");
    runInOrder(terminalId, () => forwardWithRetry(terminalId, commandCsv, log));
  });
  logger.info("gsm_downlink.forwarder_attached");
}
