// src/config/env.ts
import "dotenv/config";

const REQUIRED_VARS = [
  "DATABASE_URL",
  "DATABASE_URL_POOLED",
  "REDIS_URL",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "HIVEMQ_CLUSTER_URL",
  "HIVEMQ_PORT",
  "HIVEMQ_CLIENT_ID"
] as const;

const missing = REQUIRED_VARS.filter((key) => !process.env[key]);

if (missing.length > 0) {
  process.stderr.write(
    JSON.stringify({
      level: "fatal",
      msg: "Missing required environment variables. Halting.",
      missing,
    }) + "\n"
  );
  process.exit(1);
}

const parseIntSafe = (value: string | undefined, fallback: number): number => {
  if (!value) return fallback;
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const parseFloatSafe = (
  value: string | undefined,
  fallback: number
): number => {
  if (!value) return fallback;
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

interface Config {
  NODE_ENV: string;
  PORT: number;
  db: {
    url: string;
    pooledUrl: string;
  };
  mqtt: {
    host: string;
    port: number;
    clientId: string;
  };
  redis: {
    url: string;
  };
  ledger: {
    baseFare: number;
  };
}

const env: Config = {
  NODE_ENV: process.env.NODE_ENV || "production",
  PORT: parseIntSafe(process.env.PORT, 3001),
  db: {
    url: process.env.DATABASE_URL as string,
    pooledUrl: process.env.DATABASE_URL_POOLED as string,
  },
  mqtt: {
    host: process.env.HIVEMQ_CLUSTER_URL as string,
    port: parseIntSafe(process.env.HIVEMQ_PORT, 8883),
    clientId: process.env.HIVEMQ_CLIENT_ID as string,
  },
  redis: {
    url: process.env.REDIS_URL as string,
  },
  ledger: {
    baseFare: parseFloatSafe(process.env.BASE_FARE, 150),
  },
};

export default env;
