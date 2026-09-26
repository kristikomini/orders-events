package it.kristikomini.inventory.reservation;

/**
 * Reserves stock for a SKU. Three implementations exist — optimistic, pessimistic and (Redis)
 * distributed-lock — so the correctness/throughput trade-off can be measured rather than assumed
 * (see {@code docs/CONCURRENCY-SHOOTOUT.md}). Every implementation guarantees the same invariant:
 * <b>stock never goes negative</b>, even when many requests race for the last unit.
 */
public interface StockReserver {

    ReservationResult reserve(String sku, int quantity);

    /** A short name for logs/metrics/benchmarks (e.g. "optimistic", "pessimistic"). */
    String strategy();

    enum ReservationResult {
        RESERVED,
        OUT_OF_STOCK
    }
}
