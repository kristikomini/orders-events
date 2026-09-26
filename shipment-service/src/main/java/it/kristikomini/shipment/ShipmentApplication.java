package it.kristikomini.shipment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Shipment service — schedules a shipment when stock is reserved, and emits ShipmentScheduled. */
@SpringBootApplication
@EnableScheduling
public class ShipmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShipmentApplication.class, args);
    }
}
