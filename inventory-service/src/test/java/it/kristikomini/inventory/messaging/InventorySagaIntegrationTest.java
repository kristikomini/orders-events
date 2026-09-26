package it.kristikomini.inventory.messaging;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.inventory.domain.Stock;
import it.kristikomini.inventory.domain.StockRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The inventory saga leg over real Kafka (embedded) and real PostgreSQL (Testcontainers):
 * {@code OrderPlaced} → reserve stock → emit {@code StockReserved} via the outbox relay — and prove
 * the consumer is idempotent (a redelivered {@code OrderPlaced} does not reserve twice).
 *
 * <p>Embedded Kafka runs in-JVM (no Docker); Postgres uses Testcontainers, so the class skips
 * locally without Docker and runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1, topics = {Topics.ORDERS_PLACED, Topics.STOCK_RESERVED, Topics.STOCK_REJECTED})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "outbox.relay.fixed-delay-ms=300"
})
class InventorySagaIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired StockRepository stock;
    @Autowired EventJson json;
    @Autowired EmbeddedKafkaBroker broker;

    @BeforeEach
    void seed() {
        stock.deleteAll();
        stock.save(new Stock("SKU-1", 5));
    }

    @Test
    void reservesStockAndEmitsStockReservedIdempotently() {
        String orderId = "ord-1";
        String payload = json.toJson(new OrderEvents.OrderPlaced(orderId, "SKU-1", 1, new BigDecimal("10.00")));

        kafka.send(Topics.ORDERS_PLACED, orderId, payload);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(stock.findBySku("SKU-1").orElseThrow().getAvailable()).isEqualTo(4));

        Consumer<String, String> consumer = testConsumer();
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, Topics.STOCK_RESERVED, Duration.ofSeconds(10));
        assertThat(record.value()).contains(orderId).contains("SKU-1");
        consumer.close();

        // Redelivery of the same OrderPlaced must not reserve a second unit.
        kafka.send(Topics.ORDERS_PLACED, orderId, payload);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(stock.findBySku("SKU-1").orElseThrow().getAvailable()).isEqualTo(4));
    }

    private Consumer<String, String> testConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("verify-group", "true", broker);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        c.subscribe(List.of(Topics.STOCK_RESERVED));
        return c;
    }
}
