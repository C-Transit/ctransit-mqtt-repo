import { PrismaClient } from "@prisma/client";
import env from "../config/env.js";
import logger from "../config/logger.js";

const databaseUrl = env.NODE_ENV === "production" ? env.db.pooledUrl : env.db.url;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is not configured. Set the real PostgreSQL connection string before starting the service.");
}

// Real Prisma client. This is the single source of truth for all persistent data.
const prisma = new PrismaClient({
  log: env.NODE_ENV === "production" ? ["error"] : ["warn", "error"],
  errorFormat: "minimal",
  datasources: {
    db: {
      url: databaseUrl,
    },
  },
});

let databaseHost = "invalid-url";
try {
  databaseHost = new URL(databaseUrl).host;
} catch {
  databaseHost = "invalid-url";
}

logger.info({ databaseHost }, "prisma.connected_real_database");

export { prisma };
export default prisma;
