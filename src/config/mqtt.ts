import type { IClientOptions } from "mqtt";
import env from "./env.js";

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
    // Stable clientId — must not change between restarts.
    // Random suffix breaks clean:false session persistence
    // because HiveMQ treats each unique clientId as a new client.
    clientId: env.mqtt.clientId,
    username: env.mqtt.username,
    password: env.mqtt.password,
    protocol: "mqtts",
    port: env.mqtt.port,
    rejectUnauthorized: false,
    keepalive: 60,
    connectTimeout: 30 * 1000,
    reconnectPeriod: 5000,
    // clean: false — preserve session across restarts.
    // HiveMQ queues QoS 1 messages while service is offline
    // and delivers them in order on reconnect.
    clean: false,
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
