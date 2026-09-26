# Transactional outbox

## The problem it solves

A service that does `save to DB` and `publish to Kafka` as two separate operations can crash
between them: the order is saved but the event never publishes (lost event), or the event
publishes but the DB write rolls back (phantom event). There is no cross-system transaction to
make both-or-neither happen.

## The pattern

Write the business row **and** an `outbox` row in the **same local transaction**:

```
BEGIN
  INSERT INTO orders(...)      -- business state
  INSERT INTO outbox(...)      -- the event to publish, as a row
COMMIT
```

A separate **relay** polls (or tails, via CDC) the outbox, publishes each row to Kafka, and
marks it sent. Because the two inserts share one transaction, they are atomic; the relay makes
publishing eventually happen, at-least-once.

## Design choices

- **Polling relay** (a `@Scheduled` batch) is enough here and needs no extra infra. Debezium
  CDC is the production-grade alternative; noted as the scale-up path, not built.
- **Ordering**: publish in insertion order per aggregate (`orderId`) so consumers see a sane
  sequence.
- **Cleanup**: sent rows are archived/purged after a retention window.

## Test

Kill the relay after commit but before publish (Testcontainers), restart, assert the event is
still published exactly the business-once — the consumer dedupes any at-least-once repeats.
