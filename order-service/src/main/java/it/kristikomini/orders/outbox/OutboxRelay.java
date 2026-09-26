package it.kristikomini.orders.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Publishes pending outbox rows to Kafka, oldest first, then marks them published — all in one
 * transaction per batch here (a failed send simply leaves rows unpublished for the next poll).
 *
 * <p>The message key is the {@code orderId}, so all events for an order land on the same partition
 * and stay ordered. Consumers dedupe on {@code orderId}, so a re-published row is harmless.
 */
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
                // Block until the broker acks, so we only mark published on a real send.
                kafka.send(event.getTopic(), event.getMessageKey(), event.getPayload()).get();
                event.markPublished();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Outbox {} publish failed, will retry: {}", event.getId(), e.getMessage());
                // leave unpublished; next poll retries
            }
        }
    }
}
