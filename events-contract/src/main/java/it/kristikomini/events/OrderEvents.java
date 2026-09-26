package it.kristikomini.events;

import java.math.BigDecimal;

/**
 * The event contract shared by the services — one place both producer and consumer depend on, so
 * the wire format cannot drift between them. Plain Java records, no framework: the contract must
 * not drag Spring or JPA across service boundaries.
 *
 * <p>Every event carries the {@code orderId} as its business key; consumers dedupe on it, which is
 * what makes them idempotent under Kafka's at-least-once delivery.
 */
public final class OrderEvents {

    private OrderEvents() {
    }

    /** order-service → inventory-service: a new order needs stock reserved. */
    public record OrderPlaced(String orderId, String sku, int quantity, BigDecimal unitPrice) {
    }

    /** inventory-service → order-service/shipment-service: stock reserved for the order. */
    public record StockReserved(String orderId, String sku, int quantity) {
    }

    /** inventory-service → order-service: stock could not be reserved (compensation trigger). */
    public record StockRejected(String orderId, String sku, String reason) {
    }

    /** shipment-service → order-service: shipment scheduled, order can be confirmed. */
    public record ShipmentScheduled(String orderId, String trackingId) {
    }
}
