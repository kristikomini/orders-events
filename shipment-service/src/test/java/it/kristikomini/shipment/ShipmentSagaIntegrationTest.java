package it.kristikomini.shipment;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.shipment.domain.ShipmentRepository;
import it.kristikomini.shipment.messaging.EventJson;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shipment leg over embedded Kafka + real Postgres: {@code StockReserved} → schedule a shipment →
 * emit {@code ShipmentScheduled}, idempotently (a redelivered event does not create a 2nd shipment).
 * Skips locally without Docker; runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1, topics = {Topics.STOCK_RESERVED, Topics.SHIPMENT_SCHEDULED})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "outbox.relay.fixed-delay-ms=300"
})
class ShipmentSagaIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired ShipmentRepository shipments;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EventJson json;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void schedulesShipmentAndEmitsScheduledIdempotently() {
        String orderId = "ord-42";
        String payload = json.toJson(new OrderEvents.StockReserved(orderId, "SKU-1", 1));

        kafka.send(Topics.STOCK_RESERVED, orderId, payload);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(shipments.existsByOrderId(orderId)).isTrue());

        Consumer<String, String> consumer = consumerFor(Topics.SHIPMENT_SCHEDULED);
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(consumer, Topics.SHIPMENT_SCHEDULED, Duration.ofSeconds(10));
        assertThat(record.value()).contains(orderId).contains("TRK-");
        consumer.close();

        // Redelivery must not create a second shipment.
        kafka.send(Topics.STOCK_RESERVED, orderId, payload);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(shipments.findAll()).hasSize(1));
    }

    private Consumer<String, String> consumerFor(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("shipment-test", "true", broker);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        c.subscribe(List.of(topic));
        return c;
    }
}
