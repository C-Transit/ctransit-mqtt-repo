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
  brokerUrl: `mqtt://${env.mqtt.host}:${env.mqtt.port}`,

  options: {
    clientId: env.mqtt.clientId,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    username: (env.mqtt as any).username,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    password: (env.mqtt as any).password,
    protocol: "mqtt",
    port: env.mqtt.port,
    rejectUnauthorized: false,
    keepalive: 60,
    connectTimeout: 30 * 1000,
    reconnectPeriod: 5000,
    clean: false,
    will: {
      topic: "ctransit/server/status",
      payload: "OFFLINE", // Note: Some MQTT typings prefer Buffer.from("OFFLINE") here
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
