import mqtt from "mqtt";
import type { IConnackPacket, Packet } from "mqtt";
import mqttConfig from "../config/mqtt.js";
import logger from "../config/logger.js";
import { routeUplinkMessage } from "./uplinkRouter.js";
import { injectClient, publishToTerminal } from "./downlinkQueue.js";
import { injectMqttPublisher } from "../services/sync.service.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let client: any = null;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function connectMqtt(): Promise<any> {
  return new Promise((resolve, reject) => {
    logger.info(
      "mqtt.connecting"
    );

    client = mqtt.connect(mqttConfig.brokerUrl, {
      ...mqttConfig.options,
    });

    client.on("connect", (connack: IConnackPacket) => {
      logger.info({ sessionPresent: connack.sessionPresent }, "mqtt.connected");

      if (client) {
        injectClient(client);
        injectMqttPublisher(publishToTerminal);
      }

      const topics: Record<string, { qos: number }> = {
        [mqttConfig.topics.uplinkWildcard]: { qos: mqttConfig.qos.uplink },
        [mqttConfig.topics.statusWildcard]: { qos: mqttConfig.qos.uplink },
      };

      client?.subscribe(topics, (err: Error | null, granted?: unknown[]) => {
        if (err) {
          logger.fatal({ err: err.message }, "mqtt.subscribe_failed");
          return reject(err);
        }
        logger.info({ granted }, "mqtt.subscribed");
        if (client) resolve(client);
      });
    });

    client.on(
      "message",
      async (topic: string, payload: Buffer) => {
        const topicLog = logger.child({
          topic,
        });
        topicLog.debug("mqtt.message_received");

        try {
          await routeUplinkMessage(topic, payload);
          topicLog.debug("mqtt.message_processed");
        } catch (error) {
          const errMessage =
            error instanceof Error ? error.message : "Unknown error";
          topicLog.error({ err: errMessage }, "mqtt.message_processing_failed");
        }
      }
    );

    client.on("error", (err: Error) => {
      logger.error({ err: err.message }, "mqtt.error");
      if (client && !client.connected) reject(err);
    });

    client.on("reconnect", () => logger.warn("mqtt.reconnecting"));
    client.on("offline", () =>
      logger.warn("mqtt.offline — broker unreachable")
    );
    client.on("close", () => logger.warn("mqtt.connection_closed"));
    client.on("disconnect", (packet: Packet & { reasonCode?: number }) => {
      logger.warn(
        { reasonCode: packet.reasonCode },
        "mqtt.disconnected_by_broker"
      );
    });
  });
}

function disconnectMqtt(): Promise<void> {
  return new Promise((resolve) => {
    if (!client) return resolve();
    client.end(false, {}, () => {
      logger.info("mqtt.disconnected_gracefully");
      resolve();
    });
  });
}

export { connectMqtt, disconnectMqtt };
