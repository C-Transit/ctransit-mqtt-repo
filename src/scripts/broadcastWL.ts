import prisma from "../lib/prisma.js";
import { connectMqtt, disconnectMqtt } from "../mqtt/client.js";
import { publishToTerminal } from "../mqtt/downlinkQueue.js";
import logger from "../config/logger.js";

// Reuses the chunking logic from your uplink router
function buildChunks(
  prefix: string,
  uids: string[],
  maxBytes: number
): string[] {
  const chunks: string[] = [];
  let currentChunk: string[] = [];
  let currentSize = prefix.length;

  for (const uid of uids) {
    const segment = currentChunk.length === 0 ? uid : `|${uid}`;
    if (currentSize + segment.length > maxBytes) {
      chunks.push(`${prefix}${currentChunk.join("|")}`);
      currentChunk = [uid];
      currentSize = prefix.length + uid.length;
    } else {
      currentChunk.push(uid);
      currentSize += segment.length;
    }
  }

  if (currentChunk.length > 0) {
    chunks.push(`${prefix}${currentChunk.join("|")}`);
  }

  return chunks;
}

async function runBroadcast() {
  logger.info("Starting manual Whitelist fleet broadcast sequence...");

  try {
    // 1. Establish database connection and fetch target fleet + map data
    const terminals = await prisma.terminal.findMany({
      select: { terminal_id: true },
    });

    const cardMappings = await prisma.cardMapping.findMany({
      select: { card_uid: true },
    });

    const whitelistUids = cardMappings.map((c) => c.card_uid);

    if (terminals.length === 0) {
      logger.warn("No active terminals found in database. Aborting broadcast.");
      return;
    }

    logger.info(
      { terminalCount: terminals.length, totalCards: whitelistUids.length },
      "Fleet data successfully loaded"
    );

    // 2. Build down-link chunks safely within your MTU limit
    const MAX_CHUNK_BYTES = 500;
    const wlChunks =
      whitelistUids.length === 0
        ? ["SYS:WL,EMPTY"]
        : buildChunks("SYS:WL,", whitelistUids, MAX_CHUNK_BYTES);

    // 3. Fire up the MQTT client connection loop
    await connectMqtt();

    // 4. Distribute to all terminals sequentially
    for (const terminal of terminals) {
      const targetId = terminal.terminal_id;
      logger.info({ targetId }, "Streaming whitelist chunks to terminal");

      for (let i = 0; i < wlChunks.length; i++) {
        await publishToTerminal(targetId, wlChunks[i]);
        logger.debug(
          { targetId, chunk: i + 1, total: wlChunks.length },
          "Sent chunk"
        );

        // Pacing delay to avoid memory/buffer exhaustion on the hardware microcontrollers
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      // Signal completion token to settle the local flash memory
      await publishToTerminal(targetId, "SYS:SYNC_COMPLETE");
      logger.info({ targetId }, "Terminal sync finalized");
    }

    logger.info("Fleet broadcast complete!");
  } catch (error) {
    const msg = error instanceof Error ? error.message : "Unknown error";
    logger.error(
      { err: msg },
      "Fatal exception caught during broadcast script"
    );
  } finally {
    // 5. Unbind broker and release script thread cleanly
    await disconnectMqtt();
    process.exit(0);
  }
}

// Execute routine
runBroadcast();
