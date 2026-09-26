package it.kristikomini.orders.domain;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.orders.messaging.EventJson;
import it.kristikomini.orders.outbox.OutboxEvent;
import it.kristikomini.orders.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Places an order: persists it {@code PENDING} and writes the {@code OrderPlaced} outbox row in the
 * <b>same transaction</b>. The relay publishes it afterwards — so the order and its "please reserve
 * stock" event are atomic (no lost event, no phantom event).
 */
@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final EventJson json;

    public OrderService(OrderRepository orders, OutboxRepository outbox, EventJson json) {
        this.orders = orders;
        this.outbox = outbox;
        this.json = json;
    }

    @Transactional
    public Order place(String sku, int quantity, BigDecimal unitPrice) {
        Order order = orders.save(new Order(UUID.randomUUID().toString(), sku, quantity, unitPrice));
        OrderEvents.OrderPlaced event =
                new OrderEvents.OrderPlaced(order.getOrderId(), sku, quantity, unitPrice);
        outbox.save(new OutboxEvent(Topics.ORDERS_PLACED, order.getOrderId(), json.toJson(event)));
        return order;
    }

    @Transactional(readOnly = true)
    public Order get(String orderId) {
        return orders.findByOrderId(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
