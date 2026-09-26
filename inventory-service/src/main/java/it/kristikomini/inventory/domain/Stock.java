package it.kristikomini.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Stock level for a SKU. {@code available} must never go negative (no oversell). The
 * {@link #version} field is what the <b>optimistic</b> strategy uses to detect a concurrent
 * modification; the <b>pessimistic</b> strategy locks the row instead.
 */
@Entity
@Table(name = "stock")
public class Stock {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String sku;

    @Column(nullable = false)
    private int available;

    @Version
    @Column(nullable = false)
    private long version;

    protected Stock() {
    }

    public Stock(String sku, int available) {
        this.sku = sku;
        this.available = available;
    }

    public boolean canReserve(int quantity) {
        return available >= quantity;
    }

    /** Decrements available stock. Callers must check {@link #canReserve(int)} first. */
    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException("Cannot reserve " + quantity + " of " + sku + "; only " + available);
        }
        this.available -= quantity;
    }

    public String getSku() {
        return sku;
    }

    public int getAvailable() {
        return available;
    }
}
