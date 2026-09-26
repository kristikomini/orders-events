package it.kristikomini.events;

/** Kafka topic names — shared so producers and consumers cannot disagree on them. */
public final class Topics {

    private Topics() {
    }

    public static final String ORDERS_PLACED = "orders.placed";
    public static final String STOCK_RESERVED = "inventory.reserved";
    public static final String STOCK_REJECTED = "inventory.rejected";
    public static final String SHIPMENT_SCHEDULED = "shipments.scheduled";
}
