package it.kristikomini.inventory.reservation;

import it.kristikomini.inventory.domain.Stock;
import it.kristikomini.inventory.domain.StockRepository;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Optimistic strategy: read stock, decrement, and let JPA's {@code @Version} detect a concurrent
 * modification at commit. A conflict throws {@link OptimisticLockingFailureException}; we retry
 * with a fresh read (each attempt in its own transaction). Cheap under low contention; under high
 * contention it costs retries — the winner commits, losers re-read and see the stock is gone.
 *
 * <p>Crucially, a conflict never oversells: the loser's write is rejected, not merged.
 */
@Component("optimistic")
public class OptimisticStockReserver implements StockReserver {

    private static final int MAX_ATTEMPTS = 50;

    private final StockRepository stock;
    private final TransactionTemplate tx;

    public OptimisticStockReserver(StockRepository stock, PlatformTransactionManager txManager) {
        this.stock = stock;
        this.tx = new TransactionTemplate(txManager);
    }

    @Override
    public ReservationResult reserve(String sku, int quantity) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return tx.execute(status -> {
                    Stock s = stock.findBySku(sku)
                            .orElseThrow(() -> new IllegalStateException("Unknown SKU: " + sku));
                    if (!s.canReserve(quantity)) {
                        return ReservationResult.OUT_OF_STOCK;
                    }
                    s.reserve(quantity);
                    return ReservationResult.RESERVED;
                });
            } catch (OptimisticLockingFailureException conflict) {
                // Someone else changed the row first; re-read and try again.
            }
        }
        // Gave up after MAX_ATTEMPTS of pure contention — treat as unable to reserve now.
        return ReservationResult.OUT_OF_STOCK;
    }

    @Override
    public String strategy() {
        return "optimistic";
    }
}
