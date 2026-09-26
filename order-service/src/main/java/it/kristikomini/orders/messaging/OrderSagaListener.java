package it.kristikomini.orders.messaging;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.orders.domain.Order;
import it.kristikomini.orders.domain.OrderRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The order side of the saga. It reacts to the downstream outcomes:
 * <ul>
 *   <li>{@code ShipmentScheduled} → the order is {@code CONFIRMED};</li>
 *   <li>{@code StockRejected} → the order is {@code CANCELLED} (the compensation — no stock was
 *       committed, so nothing to release here).</li>
 * </ul>
 * Both handlers are <b>idempotent</b>: they dedupe on {@code orderId:eventType} in the same
 * transaction as the state change, so Kafka's at-least-once redelivery is harmless.
 */
@Component
public class OrderSagaListener {

    private final OrderRepository orders;
    private final ProcessedMessageRepository processed;
    private final EventJson json;

    public OrderSagaListener(OrderRepository orders, ProcessedMessageRepository processed, EventJson json) {
        this.orders = orders;
        this.processed = processed;
        this.json = json;
    }

    @KafkaListener(topics = Topics.SHIPMENT_SCHEDULED)
    @Transactional
    public void onShipmentScheduled(String payload) {
        OrderEvents.ShipmentScheduled event = json.fromJson(payload, OrderEvents.ShipmentScheduled.class);
        if (alreadyProcessed(event.orderId(), "ShipmentScheduled")) {
            return;
        }
        orders.findByOrderId(event.orderId()).ifPresent(Order::confirm);
    }

    @KafkaListener(topics = Topics.STOCK_REJECTED)
    @Transactional
    public void onStockRejected(String payload) {
        OrderEvents.StockRejected event = json.fromJson(payload, OrderEvents.StockRejected.class);
        if (alreadyProcessed(event.orderId(), "StockRejected")) {
            return;
        }
        orders.findByOrderId(event.orderId()).ifPresent(Order::cancel);
    }

    /** Records the (orderId,eventType) as processed; returns true if it was already processed. */
    private boolean alreadyProcessed(String orderId, String eventType) {
        String key = orderId + ":" + eventType;
        if (processed.existsById(key)) {
            return true;
        }
        processed.save(new ProcessedMessage(key));
        return false;
    }
}
