
# Downlink delivery

`CMD_WRAP_TERMINALS` is the comma-separated list of terminal IDs routed through
the Cloudflare Worker. Commands for those terminals are stored in the durable
Redis GSM outbox and forwarded independently of terminal online status. The
outbox entry is removed only after the Worker acknowledges it. Other terminals
continue to use the MQTT downlink queue and are flushed when they come online.

The service sends the Worker a JSON POST with `validatorId`, `commandCsv`, and
`deliveryId`, plus the `X-Worker-Secret` header. The Worker must durably accept
the command before returning HTTP success with a JSON body containing
`{"puback":true,"deliveryId":"<same deliveryId>"}`. The `deliveryId` is stable
across retries; the Worker must deduplicate repeated delivery IDs and return
the same PUBACK for a command it has already accepted. Responses without a
valid PUBACK are retried every five seconds.
