// src/mqtt/downlinkQueue.ts
import type { MqttClient } from "mqtt";
import mqttConfig from "../config/mqtt.js";
import logger from "../config/logger.js";

let _client: MqttClient | null = null;

function injectClient(client: MqttClient): void {
  _client = client;
}

// "ADD:DR,uid,pin" -> "ADD:DR". Commands can carry PINs and card UIDs, so logs use the tag only.
function commandTag(command: string): string {
  return command.split(",")[0];
}

function publishToTerminal(terminalId: string, command: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const tag = commandTag(command);

    if (!_client || !_client.connected) {
      const err = new Error(
        "MQTT client not connected — cannot publish downlink"
      );
      logger.error(
        { terminalId, command: tag, err: err.message },
        "downlink.publish_failed_no_client"
      );
      return reject(err);
    }

    const topic = mqttConfig.topics.downlink(terminalId);
    const log = logger.child({ terminalId, topic, command: tag });

    _client.publish(
      topic,
      command,
      { qos: mqttConfig.qos.downlink, retain: false },
      (err) => {
        if (err) {
          log.error({ err: err.message }, "downlink.publish_error");
          return reject(err);
        }
        log.debug("downlink.published");
        resolve();
      }
    );
  });
}

export { injectClient, publishToTerminal, commandTag };