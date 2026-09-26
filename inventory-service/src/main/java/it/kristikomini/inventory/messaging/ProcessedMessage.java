package it.kristikomini.inventory.messaging;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/** Dedupe record — a given order is reserved at most once (keyed by orderId). */
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
