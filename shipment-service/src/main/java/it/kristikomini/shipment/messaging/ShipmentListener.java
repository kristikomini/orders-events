package it.kristikomini.shipment.messaging;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.shipment.domain.Shipment;
import it.kristikomini.shipment.domain.ShipmentRepository;
import it.kristikomini.shipment.outbox.OutboxEvent;
import it.kristikomini.shipment.outbox.OutboxRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Consumes {@code StockReserved}, schedules a shipment, and emits {@code ShipmentScheduled} via the
 * outbox — in one transaction. Idempotent by the unique {@code orderId} on the shipment row: a
 * redelivered {@code StockReserved} does not create a second shipment.
 */
@Component
public class ShipmentListener {

    private final ShipmentRepository shipments;
    private final OutboxRepository outbox;
    private final EventJson json;

    public ShipmentListener(ShipmentRepository shipments, OutboxRepository outbox, EventJson json) {
        this.shipments = shipments;
        this.outbox = outbox;
        this.json = json;
    }

    @KafkaListener(topics = Topics.STOCK_RESERVED)
    @Transactional
    public void onStockReserved(String payload) {
        OrderEvents.StockReserved reserved = json.fromJson(payload, OrderEvents.StockReserved.class);
        if (shipments.existsByOrderId(reserved.orderId())) {
            return; // already scheduled — idempotent
        }
        String trackingId = "TRK-" + UUID.randomUUID().toString().substring(0, 10);
        shipments.save(new Shipment(reserved.orderId(), trackingId));
        outbox.save(new OutboxEvent(Topics.SHIPMENT_SCHEDULED, reserved.orderId(),
                json.toJson(new OrderEvents.ShipmentScheduled(reserved.orderId(), trackingId))));
    }
}
