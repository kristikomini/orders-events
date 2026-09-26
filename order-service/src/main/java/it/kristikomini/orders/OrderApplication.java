package it.kristikomini.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Order service — owns orders, produces OrderPlaced via a transactional outbox, and runs the saga. */
@SpringBootApplication
@EnableScheduling // drives the outbox relay
public class OrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
