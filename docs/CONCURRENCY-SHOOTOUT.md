# Concurrency shootout — reserving the last unit

The scenario every WMS / e-commerce backend faces: **N requests try to reserve the last 1 unit
at the same time.** Naive code oversells. This module implements three correct strategies and
benchmarks them, so the choice is evidence-based, not folklore.

## The strategies

| Strategy | How | Correct? | Cost |
|----------|-----|----------|------|
| **Optimistic** (`@Version`) | read stock, decrement, save; JPA bumps `version` and fails the losers with `OptimisticLockException` → retry | ✅ (with retry) | cheap when contention is low; retry storms when high |
| **Pessimistic** (`SELECT … FOR UPDATE`) | lock the stock row for the duration of the tx; serialise reservations | ✅ | serialises on the hot row; holds a DB lock |
| **Redis distributed lock** | acquire a lock keyed by `sku` before touching the DB | ✅ | offloads contention from the DB; adds Redis as a dependency and a fencing-token concern |

## The test that proves oversell is impossible

A single integration test (Testcontainers Postgres + Redis) that:

1. Seeds `stock(sku=X, available=1)`.
2. Uses a `CountDownLatch` (start gate) + `CyclicBarrier` so **100 threads fire the reservation
   at the same instant** — a real race, not a loop.
3. Asserts **exactly one** reservation succeeds and 99 get a clean "out of stock", and
   `available` ends at `0` (never negative).
4. Runs once per strategy; records success latency distribution and retry counts.

## Benchmark (fill after building)

| Strategy | Throughput (res/s) | p95 (ms) | Retries | Notes |
|----------|--------------------|----------|---------|-------|
| Optimistic + retry | `<N>` | `<N>` | `<N>` | |
| Pessimistic `FOR UPDATE` | `<N>` | `<N>` | 0 | |
| Redis lock | `<N>` | `<N>` | `<N>` | |

## Course topics
`CountDownLatch`, `CyclicBarrier`, `ReentrantLock`, `AtomicReference`; the `equals()`/`hashCode()`
pitfall on JPA entities; enum state machines for the order lifecycle. Academy module 08.
