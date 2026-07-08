import type { IClientOptions } from "mqtt";
import env from "./env.js";

// Define a strict interface for your MQTT setup
interface MqttConfig {
  brokerUrl: string;
  options: IClientOptions;
  topics: {
    uplinkWildcard: string;
    statusWildcard: string;
    downlink: (terminalId: string | number) => string;
  };
  qos: {
    uplink: 0 | 1 | 2;
    downlink: 0 | 1 | 2;
  };
}

const mqttConfig: MqttConfig = {
  brokerUrl: `mqtts://${env.mqtt.host}:${env.mqtt.port}`,

  options: {
    // ✅ Appends a random suffix so local and production can co-exist
    clientId: `${env.mqtt.clientId}_${Math.random()
      .toString(16)
      .substring(2, 6)}`,
    username: env.mqtt.username,
    password: env.mqtt.password,
    protocol: "mqtts",
    port: env.mqtt.port,
    rejectUnauthorized: false,
    keepalive: 60,
    connectTimeout: 30 * 1000,
    reconnectPeriod: 5000,
    clean: true,
    will: {
      topic: "ctransit/server/status",
      payload: "OFFLINE",
      qos: 1,
      retain: true,
    },
  },

  topics: {
    uplinkWildcard: "ctransit/+/tx",
    statusWildcard: "ctransit/+/status",
    downlink: (terminalId: string | number): string =>
      `ctransit/${terminalId}/rx`,
  },

  qos: {
    uplink: 1,
    downlink: 1,
  },
};

export default mqttConfig;
