package it.kristikomini.shipment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/** A scheduled shipment for an order. One per order (orderId unique). */
@Entity
@Table(name = "shipment")
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "tracking_id", nullable = false)
    private String trackingId;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    protected Shipment() {
    }

    public Shipment(String orderId, String trackingId) {
        this.orderId = orderId;
        this.trackingId = trackingId;
        this.createdAt = LocalDateTime.now();
    }

    public String getOrderId() {
        return orderId;
    }

    public String getTrackingId() {
        return trackingId;
    }
}
