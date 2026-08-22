// src/config/env.ts
import "dotenv/config";

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

const REQUIRED_VARS = [
  "DATABASE_URL",
  "DATABASE_URL_POOLED",
  "REDIS_URL",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "HIVEMQ_CLUSTER_URL",
  "HIVEMQ_PORT",
  "HIVEMQ_CLIENT_ID",
  "MQTT_INTERNAL_SECRET",
  "JWT_SECRET",
] as const;

const missing = REQUIRED_VARS.filter((key) => !process.env[key]);

if (missing.length > 0) {
  if (process.env.NODE_ENV === "production") {
    process.stderr.write(
      JSON.stringify({
        level: "fatal",
        msg: "Missing required environment variables. Halting.",
        missing,
      }) + "\n"
    );
    process.exit(1);
  }
}

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
    username: string;
    password: string;
    internalSecret: string;
  };
  redis: {
    url: string;
  };
  ledger: {
    baseFare: number;
  };
  jwt: {
    secret: string;
  };
}

const env: Config = {
  NODE_ENV: process.env.NODE_ENV || "development",
  PORT: parseIntSafe(process.env.PORT, 3000),
  db: {
    url: process.env.DATABASE_URL || "postgresql://mock:mock@localhost:5432/ctransit",
    pooledUrl: process.env.DATABASE_URL_POOLED || "postgresql://mock:mock@localhost:5432/ctransit",
  },
  mqtt: {
    host: process.env.HIVEMQ_CLUSTER_URL || "localhost",
    port: parseIntSafe(process.env.HIVEMQ_PORT, 8883),
    clientId: process.env.HIVEMQ_CLIENT_ID || "ctransit-mqtt-dev",
    username: process.env.MQTT_USERNAME || "dev_user",
    password: process.env.MQTT_PASSWORD || "dev_pass",
    internalSecret: process.env.MQTT_INTERNAL_SECRET || "dev_secret",
  },
  redis: {
    url: process.env.REDIS_URL || "redis://localhost:6379",
  },
  ledger: {
    baseFare: parseFloatSafe(process.env.BASE_FARE, 150),
  },
  jwt: {
    secret: process.env.JWT_SECRET || "dev_jwt_secret",
  },
};

export default env;

