package it.kristikomini.orders.domain;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(String orderId) {
        super("No order with id " + orderId);
    }
}
