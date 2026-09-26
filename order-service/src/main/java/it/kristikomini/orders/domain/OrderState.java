package it.kristikomini.orders.domain;

/** Order lifecycle in the saga: PENDING until stock+shipment succeed (CONFIRMED) or stock fails (CANCELLED). */
public enum OrderState {
    PENDING,
    CONFIRMED,
    CANCELLED
}
