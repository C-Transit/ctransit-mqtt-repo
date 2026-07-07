import { PrismaClient } from "@prisma/client";
import env from "../config/env.js";

const isProduction = env.NODE_ENV === "production";

// ✅ Connection strategy:
//   Production  → pooled URL (PgBouncer on Neon) — handles high concurrency
//   Development → direct URL — supports all Prisma features (no pgbouncer limitations)
const databaseUrl = isProduction ? env.db.pooledUrl : env.db.url;

const prisma = new PrismaClient({
  // ✅ Log slow queries and errors in dev, errors only in production
  log: isProduction ? ["error"] : ["warn", "error"],

  errorFormat: "minimal", // Shorter errors — better for structured logging

  datasources: {
    db: {
      url: databaseUrl,
    },
  },
});

export default prisma;
