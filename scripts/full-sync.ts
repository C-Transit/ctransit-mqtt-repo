#!/usr/bin/env tsx
/**
 * full-sync.ts — push a full whitelist/blacklist sync to one terminal.
 *
 * Two modes
 * ---------
 * request (default)
 *   Plays the part of the terminal: publishes SYS:REQ_FULL_SYNC to
 *   ctransit/<TERMINAL_ID>/tx. The running service sees it, reads its own
 *   database and pushes SYS:BL / SYS:WL / SYS:SYNC_COMPLETE. The script
 *   subscribes to ctransit/<TERMINAL_ID>/rx to confirm what arrived.
 *   No database access from this machine.
 *
 * direct (--direct)
 *   Reads the real whitelist and blacklist with Prisma and publishes the real
 *   SYS:BL / SYS:WL chunks and SYS:SYNC_COMPLETE itself, in the same order and
 *   pacing as syncFullListsToTerminal(). Use this when you want the data to
 *   come from this machine (for example to verify exactly what is sent).
 *   Requires DATABASE_URL (falls back to DATABASE_URL_POOLED).
 *
 * Neither mode writes to the database.
 *
 * Usage
 * -----
 *   npx tsx scripts/full-sync.ts 9AF7F0
 *   npx tsx scripts/full-sync.ts 9AF7F0 --direct
 *   npx tsx scripts/full-sync.ts 9AF7F0 --direct --dry-run
 *   npx tsx scripts/full-sync.ts 9AF7F0 --timeout 60
 *   npx tsx scripts/full-sync.ts 9AF7F0 --no-watch
 *   npx tsx scripts/full-sync.ts 9AF7F0 --env-file .env.production
 *   npm run full-sync -- 9AF7F0 --direct
 *
 * Environment (from the shell or --env-file):
 *   HIVEMQ_CLUSTER_URL   broker host, with or without mqtts://
 *   MQTT_USERNAME        broker username
 *   MQTT_PASSWORD        broker password
 *   HIVEMQ_PORT          optional, defaults to 8883
 *   DATABASE_URL         direct mode only (DATABASE_URL_POOLED is the fallback)
 *   CMD_WRAP_TERMINALS   direct mode: wrap SYS:SYNC_COMPLETE for these ids
 */

import { randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { config as loadEnv } from "dotenv";
import mqtt from "mqtt";
import type { MqttClient } from "mqtt";
import { PrismaClient } from "@prisma/client";
import { buildChunks } from "../src/utils/parser.js";

const TERMINAL_ID_PATTERN = /^[A-Za-z0-9_-]{1,20}$/;
const RESERVED_IDS = new Set(["SERVER", "SYSTEM_TERMINAL"]);
const WRAPPED_PREFIX = /^CMD:[0-9a-f]{12};/;
const DEFAULT_TIMEOUT_SECONDS = 45;

// Must match syncFullListsToTerminal().
const MAX_CHUNK_BYTES = 500;
const SYNC_DELAY_MS = 200;

const USAGE = `Usage: npx tsx scripts/full-sync.ts <TERMINAL_ID> [options]

Modes:
  (default)             ask the running service to send the sync (real data from its DB)
  --direct              send the sync from this machine (real data from DATABASE_URL)

Options:
  --timeout <seconds>   how long request mode waits for SYS:SYNC_COMPLETE (default ${DEFAULT_TIMEOUT_SECONDS})
  --no-watch            request mode: publish and exit without waiting for the rx reply
  --dry-run             show what would be sent, publish nothing (direct mode still reads the DB)
  --wrap                direct mode: wrap SYS:SYNC_COMPLETE as CMD:<12hex>;...
  --no-wrap             direct mode: never wrap
  --env-file <path>     load variables from this file (default: .env if present)
  -h, --help            show this help`;

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

function fail(message: string): never {
  process.stderr.write(`error: ${message}\n`);
  process.exit(1);
}

// "SYS:BL,uid|uid" / "CMD:<12hex>;SYS:SYNC_COMPLETE" -> "SYS:BL" / "SYS:SYNC_COMPLETE".
// Only the tag is ever printed; UIDs and chunk bodies stay out of the output.
function commandTagOf(payload: string): string {
  return payload.replace(WRAPPED_PREFIX, "").split(",")[0].trim();
}

function countUids(payload: string): number {
  const body = payload.replace(WRAPPED_PREFIX, "");
  const list = body.substring(body.indexOf(",") + 1).trim();
  if (!list || list.toUpperCase() === "EMPTY") return 0;
  return list.split("|").filter((uid) => uid.trim().length > 0).length;
}

function parseIntArg(raw: string | undefined, flag: string): number {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    fail(`${flag} needs a positive number of seconds`);
  }
  return Math.floor(value);
}

function resolveBrokerUrl(): string {
  const raw = process.env.HIVEMQ_CLUSTER_URL;
  if (!raw) fail("HIVEMQ_CLUSTER_URL is not set");

  const match = /^(?:[a-z]+:\/\/)?([^/:]+)(?::(\d+))?/i.exec(raw.trim());
  if (!match) fail("HIVEMQ_CLUSTER_URL is not a valid host");

  const host = match[1];
  const port = match[2] ?? process.env.HIVEMQ_PORT ?? "8883";
  return `mqtts://${host}:${port}`;
}

function safeHost(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return "invalid-url";
  }
}

function connect(client: MqttClient, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = (): void => {
      clearTimeout(timer);
      client.removeListener("connect", onConnect);
      client.removeListener("error", onError);
    };
    const onConnect = (): void => {
      cleanup();
      resolve();
    };
    const onError = (err: Error): void => {
      cleanup();
      reject(err);
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`broker did not accept the connection within ${timeoutMs / 1000}s`));
    }, timeoutMs);

    client.once("connect", onConnect);
    client.once("error", onError);
  });
}

function subscribe(client: MqttClient, topic: string): Promise<void> {
  return new Promise((resolve, reject) => {
    client.subscribe(topic, { qos: 1 }, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

function publish(client: MqttClient, topic: string, payload: string): Promise<void> {
  return new Promise((resolve, reject) => {
    client.publish(topic, payload, { qos: 1, retain: false }, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

function end(client: MqttClient): Promise<void> {
  return new Promise((resolve) => {
    client.end(false, {}, () => resolve());
  });
}

function makeClient(brokerUrl: string): MqttClient {
  const username = process.env.MQTT_USERNAME;
  const password = process.env.MQTT_PASSWORD;
  if (!username || !password) {
    fail("MQTT_USERNAME and MQTT_PASSWORD must be set (shell or --env-file)");
  }

  // A fresh client id every run: reusing HIVEMQ_CLIENT_ID would kick the
  // service's own session off the broker.
  const client = mqtt.connect(brokerUrl, {
    clientId: `fullsync-${randomBytes(4).toString("hex")}`,
    username,
    password,
    protocol: "mqtts",
    rejectUnauthorized: true,
    clean: true,
    keepalive: 30,
    connectTimeout: 15000,
    reconnectPeriod: 0,
  });

  client.on("error", (err: Error) => {
    process.stderr.write(`broker error: ${err.message}\n`);
  });

  return client;
}

interface RxSummary {
  messages: number;
  blChunks: number;
  wlChunks: number;
  blUids: number;
  wlUids: number;
  other: number;
  complete: boolean;
}

interface DirectLists {
  whitelist: string[];
  blacklist: string[];
}

async function loadDirectLists(url: string): Promise<DirectLists> {
  const prisma = new PrismaClient({
    datasources: { db: { url } },
    log: ["error"],
  });

  try {
    // Same queries as syncFullListsToTerminal()
    const cardMappings = await prisma.cardMapping.findMany({
      select: { card_uid: true },
    });

    const blacklistedStudents = await prisma.blacklist.findMany({
      select: { student_uid: true },
    });

    let blacklistUids: string[] = [];
    if (blacklistedStudents.length > 0) {
      const blacklistCards = await prisma.cardMapping.findMany({
        where: { student_uid: { in: blacklistedStudents.map((b) => b.student_uid) } },
        select: { card_uid: true },
      });
      blacklistUids = blacklistCards.map((c) => c.card_uid);
    }

    return {
      whitelist: cardMappings.map((c) => c.card_uid),
      blacklist: blacklistUids,
    };
  } finally {
    await prisma.$disconnect();
  }
}

function maybeWrap(terminalId: string, command: string, wrapOverride: boolean | null): string {
  const fromEnv = (process.env.CMD_WRAP_TERMINALS ?? "")
    .split(",")
    .map((id) => id.trim().toUpperCase())
    .filter((id) => id.length > 0)
    .includes(terminalId);

  const wrap = wrapOverride === null ? fromEnv : wrapOverride;
  return wrap ? `CMD:${randomBytes(6).toString("hex")};${command}` : command;
}

async function runDirect(options: {
  terminalId: string;
  brokerUrl: string;
  rxTopic: string;
  dryRun: boolean;
  wrapOverride: boolean | null;
}): Promise<number> {
  const { terminalId, brokerUrl, rxTopic, dryRun, wrapOverride } = options;

  const databaseUrl = process.env.DATABASE_URL ?? process.env.DATABASE_URL_POOLED;
  if (!databaseUrl) {
    fail("direct mode needs DATABASE_URL (or DATABASE_URL_POOLED)");
  }

  process.stdout.write(`mode       direct (reading the database from this machine)\n`);
  process.stdout.write(`database   ${safeHost(databaseUrl)}\n`);

  let lists: DirectLists;
  try {
    lists = await loadDirectLists(databaseUrl);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`could not read the database: ${message}\n`);
    return 1;
  }

  const blChunks =
    lists.blacklist.length === 0
      ? ["SYS:BL,EMPTY"]
      : buildChunks("SYS:BL,", lists.blacklist, MAX_CHUNK_BYTES);
  const wlChunks =
    lists.whitelist.length === 0
      ? ["SYS:WL,EMPTY"]
      : buildChunks("SYS:WL,", lists.whitelist, MAX_CHUNK_BYTES);
  const complete = maybeWrap(terminalId, "SYS:SYNC_COMPLETE", wrapOverride);

  process.stdout.write(
    `blacklist  ${lists.blacklist.length} uid(s) -> ${blChunks.length} chunk(s)\n`
  );
  process.stdout.write(
    `whitelist  ${lists.whitelist.length} uid(s) -> ${wlChunks.length} chunk(s)\n`
  );
  process.stdout.write(
    `complete   ${WRAPPED_PREFIX.test(complete) ? "wrapped CMD:<id>;SYS:SYNC_COMPLETE" : "plain SYS:SYNC_COMPLETE"}\n`
  );
  process.stdout.write(`publish    ${rxTopic} (qos 1, ${MAX_CHUNK_BYTES}-byte chunks)\n`);

  if (dryRun) {
    process.stdout.write("dry run — nothing was published\n");
    return 0;
  }

  const client = makeClient(brokerUrl);

  // Our own publishes come back to a subscriber on the same topic, so direct
  // mode does not watch; the publish callbacks already confirm each chunk.

  try {
    await connect(client, 15000);
    process.stdout.write("connected\n");
  } catch (err) {
    await end(client);
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`could not connect: ${message}\n`);
    return 1;
  }

  try {
    for (let i = 0; i < blChunks.length; i++) {
      await publish(client, rxTopic, blChunks[i]);
      process.stdout.write(
        `  -> SYS:BL  chunk ${i + 1}/${blChunks.length} (${countUids(blChunks[i])} uid(s))\n`
      );
      if (i < blChunks.length - 1) await sleep(SYNC_DELAY_MS);
    }

    await sleep(SYNC_DELAY_MS);

    for (let i = 0; i < wlChunks.length; i++) {
      await publish(client, rxTopic, wlChunks[i]);
      process.stdout.write(
        `  -> SYS:WL  chunk ${i + 1}/${wlChunks.length} (${countUids(wlChunks[i])} uid(s))\n`
      );
      if (i < wlChunks.length - 1) await sleep(SYNC_DELAY_MS);
    }

    await sleep(SYNC_DELAY_MS);
    await publish(client, rxTopic, complete);
    process.stdout.write("  -> SYS:SYNC_COMPLETE\n");
  } catch (err) {
    await end(client);
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`publish failed: ${message}\n`);
    return 1;
  }

  await end(client);
  process.stdout.write("full sync delivered\n");
  return 0;
}

async function runRequest(options: {
  terminalId: string;
  brokerUrl: string;
  txTopic: string;
  rxTopic: string;
  timeoutSeconds: number;
  watch: boolean;
  dryRun: boolean;
}): Promise<number> {
  const { terminalId, brokerUrl, txTopic, rxTopic, timeoutSeconds, watch, dryRun } = options;
  const request = "SYS:REQ_FULL_SYNC";

  process.stdout.write(`mode       request (the running service sends the sync)\n`);
  process.stdout.write(`publish    ${request} -> ${txTopic} (qos 1)\n`);
  process.stdout.write(`watch      ${rxTopic}\n`);

  if (dryRun) {
    process.stdout.write("dry run — nothing was published\n");
    return 0;
  }

  const client = makeClient(brokerUrl);

  const summary: RxSummary = {
    messages: 0,
    blChunks: 0,
    wlChunks: 0,
    blUids: 0,
    wlUids: 0,
    other: 0,
    complete: false,
  };

  try {
    await connect(client, 15000);
    process.stdout.write("connected\n");
  } catch (err) {
    await end(client);
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`could not connect: ${message}\n`);
    return 1;
  }

  if (watch) {
    client.on("message", (topic: string, payload: Buffer) => {
      if (topic !== rxTopic) return;
      const text = payload.toString("utf8").trim();
      if (!text) return;

      summary.messages++;
      const tag = commandTagOf(text);

      if (tag === "SYS:BL") {
        const uids = countUids(text);
        summary.blChunks++;
        summary.blUids += uids;
        process.stdout.write(`  <- SYS:BL  ${uids} uid(s)\n`);
      } else if (tag === "SYS:WL") {
        const uids = countUids(text);
        summary.wlChunks++;
        summary.wlUids += uids;
        process.stdout.write(`  <- SYS:WL  ${uids} uid(s)\n`);
      } else if (tag === "SYS:SYNC_COMPLETE") {
        summary.complete = true;
        process.stdout.write("  <- SYS:SYNC_COMPLETE\n");
      } else {
        summary.other++;
        process.stdout.write(`  <- ${tag || "(unparsed)"}  ${text.length} bytes\n`);
      }
    });

    try {
      await subscribe(client, rxTopic);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`warning: could not subscribe to ${rxTopic}: ${message}\n`);
    }
  }

  try {
    await publish(client, txTopic, request);
    process.stdout.write("request published\n");
  } catch (err) {
    await end(client);
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`publish failed: ${message}\n`);
    return 1;
  }

  if (!watch) {
    await end(client);
    return 0;
  }

  const deadline = Date.now() + timeoutSeconds * 1000;
  while (!summary.complete && Date.now() < deadline) {
    await sleep(200);
  }

  await end(client);

  process.stdout.write(
    `received   ${summary.messages} message(s): ` +
      `${summary.blChunks} BL chunk(s)/${summary.blUids} uid(s), ` +
      `${summary.wlChunks} WL chunk(s)/${summary.wlUids} uid(s), ` +
      `${summary.other} other\n`
  );

  if (summary.complete) {
    process.stdout.write("full sync delivered\n");
    return 0;
  }

  process.stderr.write(
    `no SYS:SYNC_COMPLETE within ${timeoutSeconds}s. ` +
      "Check that the service is running and subscribed to ctransit/+/tx, " +
      "and that the terminal is online.\n"
  );
  return 2;
}

async function main(): Promise<number> {
  const argv = process.argv.slice(2);
  let envFile: string | undefined;
  let timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
  let watch = true;
  let dryRun = false;
  let direct = false;
  let wrapOverride: boolean | null = null;
  const positional: string[] = [];

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "-h" || arg === "--help") {
      process.stdout.write(`${USAGE}\n`);
      return 0;
    } else if (arg === "--dry-run") {
      dryRun = true;
    } else if (arg === "--no-watch") {
      watch = false;
    } else if (arg === "--direct") {
      direct = true;
    } else if (arg === "--wrap") {
      wrapOverride = true;
    } else if (arg === "--no-wrap") {
      wrapOverride = false;
    } else if (arg === "--env-file") {
      envFile = argv[++i];
      if (!envFile) fail("--env-file needs a path");
    } else if (arg === "--timeout") {
      timeoutSeconds = parseIntArg(argv[++i], "--timeout");
    } else if (arg.startsWith("--")) {
      fail(`unknown option ${arg}\n\n${USAGE}`);
    } else {
      positional.push(arg);
    }
  }

  if (wrapOverride !== null && !direct) {
    fail("--wrap/--no-wrap only apply to --direct mode");
  }

  const terminalId = (positional[0] ?? "").toUpperCase();
  if (!TERMINAL_ID_PATTERN.test(terminalId)) {
    fail(`expected one terminal id (A-Z, 0-9, _ or -, up to 20 chars)\n\n${USAGE}`);
  }
  if (RESERVED_IDS.has(terminalId)) {
    fail(`${terminalId} is not a physical terminal`);
  }

  if (envFile) {
    if (!existsSync(envFile)) fail(`env file not found: ${envFile}`);
    loadEnv({ path: envFile, quiet: true });
  } else if (existsSync(".env")) {
    loadEnv({ quiet: true });
  }

  const brokerUrl = resolveBrokerUrl();
  const txTopic = `ctransit/${terminalId}/tx`;
  const rxTopic = `ctransit/${terminalId}/rx`;

  process.stdout.write(`terminal   ${terminalId}\n`);
  process.stdout.write(`broker     ${brokerUrl}\n`);

  if (direct) {
    return runDirect({
      terminalId,
      brokerUrl,
      rxTopic,
      dryRun,
      wrapOverride,
    });
  }

  return runRequest({
    terminalId,
    brokerUrl,
    txTopic,
    rxTopic,
    timeoutSeconds,
    watch,
    dryRun,
  });
}

main()
  .then((code) => process.exit(code))
  .catch((err: unknown) => {
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`unexpected error: ${message}\n`);
    process.exit(1);
  });
