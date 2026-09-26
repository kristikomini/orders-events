# Idempotent consumers

Kafka delivers **at least once**. A consumer will occasionally see the same message twice
(rebalance, retry, relay republish). Processing it twice must have the same effect as once.

## Technique

Dedupe on a **business key**, not the Kafka offset:

- `inventory-service` records `processed_message(order_id PRIMARY KEY)` in the same transaction
  as the stock reservation. A second delivery hits the primary-key constraint → skip.
- Reservations are keyed by `orderId` so a retried `OrderPlaced` reserves once.

## Why not just rely on offsets / exactly-once?

Kafka EOS covers Kafka-to-Kafka; the moment you touch a database the guarantee is yours to
build. A dedupe table in the consumer's own DB, committed with the side effect, is the honest
boundary — and it survives consumer restarts and topic replays.

## Test

Publish the same `OrderPlaced` `N` times; assert exactly one reservation and one `StockReserved`.
