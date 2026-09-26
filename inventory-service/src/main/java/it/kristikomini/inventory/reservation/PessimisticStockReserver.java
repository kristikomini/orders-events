package it.kristikomini.inventory.reservation;

import it.kristikomini.inventory.domain.Stock;
import it.kristikomini.inventory.domain.StockRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Pessimistic strategy: lock the stock row {@code SELECT … FOR UPDATE} for the duration of the
 * transaction, so concurrent reservers serialise on the row. Correct with zero retries; the cost
 * is that reservations for a hot SKU are serialised and a DB lock is held while deciding.
 */
@Component("pessimistic")
public class PessimisticStockReserver implements StockReserver {

    private final StockRepository stock;
    private final TransactionTemplate tx;

    public PessimisticStockReserver(StockRepository stock, PlatformTransactionManager txManager) {
        this.stock = stock;
        this.tx = new TransactionTemplate(txManager);
    }

    @Override
    public ReservationResult reserve(String sku, int quantity) {
        return tx.execute(status -> {
            Stock s = stock.lockBySku(sku)
                    .orElseThrow(() -> new IllegalStateException("Unknown SKU: " + sku));
            if (!s.canReserve(quantity)) {
                return ReservationResult.OUT_OF_STOCK;
            }
            s.reserve(quantity);
            return ReservationResult.RESERVED;
        });
    }

    @Override
    public String strategy() {
        return "pessimistic";
    }
}
