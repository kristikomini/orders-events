# Event-driven orders & inventory — Kafka, outbox, saga

Three Spring Boot services that coordinate over **Apache Kafka** the way it should be done in
production: not fire-and-forget, but with a **transactional outbox** (no lost events),
**idempotent consumers** (no double-processing on redelivery), a **reservation saga** with
compensation (no stuck orders), and a **concurrency shootout** that proves inventory can never
oversell the last unit. Redis, correlation IDs and Prometheus metrics throughout.

Target market: Packaging Valley / industrial logistics & WMS, e-commerce and manufacturing
(Modena, Bologna, Verona). "Microservices + Kafka" is on nearly every advert; the differentiator
is doing it *correctly*.

> Follows the shared [engineering standards](../ENGINEERING-STANDARDS.md).

## The services

| Module | Responsibility |
|--------|----------------|
| `events-contract` | Shared event schema (the contract between services). Versioned records; one place both sides depend on. |
| `order-service` | Owns orders. Accepts an order, writes it + an `OrderPlaced` event **in one transaction** (outbox), then a relay publishes to Kafka. Consumes stock/shipment outcomes to drive the order state machine. |
| `inventory-service` | Owns stock. Consumes `OrderPlaced`, reserves stock **idempotently** under real concurrency (optimistic / pessimistic / Redis-lock — see the shootout), emits `StockReserved` or `StockRejected`. |
| `shipment-service` | Consumes `StockReserved`, schedules a shipment, emits `ShipmentScheduled` — the third saga participant, and where a compensating rollback is triggered on failure. |

## The flow (a saga)

```
order-service                    Kafka                     inventory-service
  POST /orders
  ├─ INSERT order (PENDING) ┐
  └─ INSERT outbox row      ┘ one tx
        relay ──OrderPlaced──▶ topic ──▶ reserve stock (idempotent by orderId)
                                              ├─ ok  ──StockReserved──▶ topic ──▶ order → CONFIRMED
                                              └─ no  ──StockRejected──▶ topic ──▶ order → CANCELLED
```

If a step fails or a message is redelivered, the outcome is the same — that is the whole point.

## The four things this gets right

1. **Transactional outbox.** The business write and the "event to publish" commit atomically;
   a separate relay reads the outbox and publishes. Kafka being down cannot lose an event, and
   the service never dual-writes to DB and broker in a way that can half-fail.
   → [`docs/OUTBOX.md`](docs/OUTBOX.md)
2. **Idempotent consumers.** Kafka is at-least-once, so consumers dedupe on a business key
   (`orderId`) and treat reprocessing as a no-op. → [`docs/IDEMPOTENCY.md`](docs/IDEMPOTENCY.md)
3. **No oversell under concurrency.** Reserving the last unit is implemented three ways —
   optimistic `@Version`, pessimistic `SELECT … FOR UPDATE`, and a Redis distributed lock — and
   a `CountDownLatch`/`CyclicBarrier` test fires 100 simultaneous requests at 1 unit to prove
   exactly one wins. → [`docs/CONCURRENCY-SHOOTOUT.md`](docs/CONCURRENCY-SHOOTOUT.md)
4. **Saga with compensation.** No distributed transaction across services; each local step is
   committed and a failure downstream (payment/shipment) triggers a compensating event that
   releases the reserved stock.

## What this demonstrates (CV bullets)

*Proven by tests in this repo (embedded Kafka in-JVM; Postgres via Testcontainers in CI):*
- Built three event-driven Spring Boot services over Kafka with a **transactional outbox** in each
  producer, eliminating lost-event dual-write failures; the whole `OrderPlaced → reserve →
  StockReserved` leg is verified against embedded Kafka + real Postgres.
- Made consumers **idempotent** (dedupe by `orderId`), proven by replaying a duplicate `OrderPlaced`
  and asserting no second reservation.
- Guaranteed **zero oversell** of the last unit under **100 concurrent requests** with all three
  strategies — optimistic (`@Version` + retry), pessimistic (`FOR UPDATE`) and a **Redis
  distributed lock** — verified against real Postgres + Redis (exactly one wins, stock ends at 0).
- Implemented an order → stock → shipment **saga** with compensation, each leg covered by an
  integration test (embedded Kafka + Testcontainers Postgres): `OrderPlaced → StockReserved →
  ShipmentScheduled → order CONFIRMED`, and `StockRejected → order CANCELLED`.

*To fill in once benchmarked:* strategy throughput (res/s) and end-to-end saga p95.

## Run it

```bash
docker compose up   # kafka + postgres(x3) + redis + order/inventory/shipment + prometheus
```

## Course modules exercised

26 (messaging & the outbox), 08 (concurrency), 17 (the request cycle), 23 (transactions),
27 (keeping it up — health, metrics, correlation IDs).
