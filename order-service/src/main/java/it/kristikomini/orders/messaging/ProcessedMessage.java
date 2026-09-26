package it.kristikomini.orders.messaging;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/**
 * Dedupe record for idempotent consumers. Kafka is at-least-once, so a consumer may see the same
 * event twice; the handler inserts one of these (keyed by {@code orderId:eventType}) in the same
 * transaction as its side effect. A duplicate delivery hits the primary key and is skipped, so
 * reprocessing has no additional effect.
 */
@Entity
@Table(name = "processed_message")
public class ProcessedMessage {

    @Id
    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    protected ProcessedMessage() {
    }

    public ProcessedMessage(String messageKey) {
        this.messageKey = messageKey;
        this.processedAt = LocalDateTime.now();
    }
}
