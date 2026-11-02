
# Downlink delivery

`CMD_WRAP_TERMINALS` is the comma-separated list of terminal IDs routed through
the Cloudflare Worker. Commands for those terminals are stored in the durable
Redis GSM outbox and forwarded independently of terminal online status. The
outbox entry is removed only after the Worker acknowledges it. Other terminals
continue to use the MQTT downlink queue and are flushed when they come online.

The service sends the Worker a JSON POST with `validatorId`, `commandCsv`, and
`deliveryId`, plus the `X-Worker-Secret` header. The Worker must durably store
the command before returning HTTP success with
`{"accepted":true,"deliveryId":"<same deliveryId>"}`. The `deliveryId` is
stable across retries so the Worker can deduplicate repeated requests. The
backend also accepts the deployed Worker’s legacy `OK` and `ALREADY_QUEUED`
plain-text responses. Responses without a valid acknowledgment are retried
every five seconds.
