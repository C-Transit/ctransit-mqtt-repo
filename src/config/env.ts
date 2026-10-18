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
] as const;

const isProduction = process.env.NODE_ENV === "production";

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

// In production, prohibit development fallback/default secrets
if (isProduction) {
  const disallowedDefaults: Record<string, string> = {
    MQTT_INTERNAL_SECRET: "dev_secret",
    MQTT_PASSWORD: "dev_pass",
    MQTT_USERNAME: "dev_user",
  };

  const detectedDefaults = Object.entries(disallowedDefaults)
    .filter(([varName, defVal]) => process.env[varName] === defVal)
    .map(([varName]) => varName);

  if (detectedDefaults.length > 0) {
    process.stderr.write(
      JSON.stringify({
        level: "fatal",
        msg: "Insecure default/fallback secrets detected in production environment. Halting.",
        variables: detectedDefaults,
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
  terminals: {
    autoRegister: boolean;
  };
}

const env: Config = {
  NODE_ENV: process.env.NODE_ENV || "development",
  PORT: parseIntSafe(process.env.PORT, 3000),
  db: {
    url: process.env.DATABASE_URL as string,
    pooledUrl: process.env.DATABASE_URL_POOLED as string,
  },
  mqtt: {
    host: process.env.HIVEMQ_CLUSTER_URL as string,
    port: parseIntSafe(process.env.HIVEMQ_PORT, 8883),
    clientId: process.env.HIVEMQ_CLIENT_ID as string,
    username: process.env.MQTT_USERNAME as string,
    password: process.env.MQTT_PASSWORD as string,
    internalSecret: process.env.MQTT_INTERNAL_SECRET as string,
  },
  redis: {
    url: process.env.REDIS_URL as string,
  },
  ledger: {
    baseFare: parseFloatSafe(process.env.BASE_FARE, 150),
  },
  terminals: {
    // Set AUTO_REGISTER_TERMINALS=false once every real terminal exists in the database.
    autoRegister: process.env.AUTO_REGISTER_TERMINALS !== "false",
  },
};

export default env;

