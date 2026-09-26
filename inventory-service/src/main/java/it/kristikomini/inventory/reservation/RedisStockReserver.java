package it.kristikomini.inventory.reservation;

import it.kristikomini.inventory.domain.Stock;
import it.kristikomini.inventory.domain.StockRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.UUID;

/**
 * Distributed-lock strategy: acquire a Redis lock on the SKU before touching the database, so the
 * contention is absorbed by Redis instead of the DB row. Useful when many app instances compete and
 * you want to keep DB lock time minimal, or coordinate across resources the DB alone cannot.
 *
 * <p>The lock is a {@code SET key value NX PX ttl} with a unique token; release only deletes the key
 * if we still own it (checked here in two steps — a production version uses a Lua compare-and-delete
 * for atomicity, and a fencing token to survive a lock timeout). The DB decrement still runs in a
 * transaction, so it is durable; the lock only provides mutual exclusion.
 */
@Component("redis")
public class RedisStockReserver implements StockReserver {

    private static final Duration LOCK_TTL = Duration.ofSeconds(10);
    private static final Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(10);

    private final StockRepository stock;
    private final StringRedisTemplate redis;
    private final TransactionTemplate tx;

    public RedisStockReserver(StockRepository stock, StringRedisTemplate redis,
                              PlatformTransactionManager txManager) {
        this.stock = stock;
        this.redis = redis;
        this.tx = new TransactionTemplate(txManager);
    }

    @Override
    public ReservationResult reserve(String sku, int quantity) {
        String key = "lock:stock:" + sku;
        String token = UUID.randomUUID().toString();
        if (!acquire(key, token)) {
            return ReservationResult.OUT_OF_STOCK; // could not get the lock in time
        }
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
        } finally {
            release(key, token);
        }
    }

    private boolean acquire(String key, String token) {
        long deadline = System.nanoTime() + ACQUIRE_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            Boolean ok = redis.opsForValue().setIfAbsent(key, token, LOCK_TTL);
            if (Boolean.TRUE.equals(ok)) {
                return true;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void release(String key, String token) {
        // Only release the lock if we still own it (best-effort; Lua would make this atomic).
        if (token.equals(redis.opsForValue().get(key))) {
            redis.delete(key);
        }
    }

    @Override
    public String strategy() {
        return "redis";
    }
}
