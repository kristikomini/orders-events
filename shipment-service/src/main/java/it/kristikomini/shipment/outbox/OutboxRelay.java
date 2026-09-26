package it.kristikomini.shipment.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Publishes shipment's pending outbox rows (ShipmentScheduled) to Kafka. */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka) {
        this.outbox = outbox;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending = outbox.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(100));
        for (OutboxEvent event : pending) {
            try {
                event.recordAttempt();
                kafka.send(event.getTopic(), event.getMessageKey(), event.getPayload()).get();
                event.markPublished();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Outbox {} publish failed, will retry: {}", event.getId(), e.getMessage());
            }
        }
    }
}
