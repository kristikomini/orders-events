package it.kristikomini.inventory.messaging;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.inventory.outbox.OutboxEvent;
import it.kristikomini.inventory.outbox.OutboxRepository;
import it.kristikomini.inventory.reservation.StockReserver;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes {@code OrderPlaced}, reserves stock, and emits {@code StockReserved} or
 * {@code StockRejected} via the outbox — all in one transaction, so the reservation, the dedupe
 * record and the outgoing event commit together.
 *
 * <p>Idempotent: dedupes on {@code orderId}, so a redelivered {@code OrderPlaced} does not reserve
 * twice. The pessimistic reserver is used by default (correct with no retries under contention).
 */
@Component
public class InventoryListener {

    private final StockReserver reserver;
    private final OutboxRepository outbox;
    private final ProcessedMessageRepository processed;
    private final EventJson json;

    public InventoryListener(@Qualifier("pessimistic") StockReserver reserver,
                             OutboxRepository outbox,
                             ProcessedMessageRepository processed,
                             EventJson json) {
        this.reserver = reserver;
        this.outbox = outbox;
        this.processed = processed;
        this.json = json;
    }

    @KafkaListener(topics = Topics.ORDERS_PLACED)
    @Transactional
    public void onOrderPlaced(String payload) {
        OrderEvents.OrderPlaced order = json.fromJson(payload, OrderEvents.OrderPlaced.class);
        if (processed.existsById(order.orderId())) {
            return; // already handled this order — idempotent no-op
        }
        processed.save(new ProcessedMessage(order.orderId()));

        StockReserver.ReservationResult result = reserver.reserve(order.sku(), order.quantity());
        if (result == StockReserver.ReservationResult.RESERVED) {
            outbox.save(new OutboxEvent(Topics.STOCK_RESERVED, order.orderId(),
                    json.toJson(new OrderEvents.StockReserved(order.orderId(), order.sku(), order.quantity()))));
        } else {
            outbox.save(new OutboxEvent(Topics.STOCK_REJECTED, order.orderId(),
                    json.toJson(new OrderEvents.StockRejected(order.orderId(), order.sku(), "OUT_OF_STOCK"))));
        }
    }
}
