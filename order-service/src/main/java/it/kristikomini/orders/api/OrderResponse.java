package it.kristikomini.orders.api;

import it.kristikomini.orders.domain.Order;

import java.math.BigDecimal;

public record OrderResponse(String orderId, String sku, int quantity, BigDecimal unitPrice, String state) {

    public static OrderResponse of(Order order) {
        return new OrderResponse(order.getOrderId(), order.getSku(), order.getQuantity(),
                order.getUnitPrice(), order.getState().name());
    }
}
