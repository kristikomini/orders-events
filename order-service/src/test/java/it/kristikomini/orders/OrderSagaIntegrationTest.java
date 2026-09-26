package it.kristikomini.orders;

import it.kristikomini.events.OrderEvents;
import it.kristikomini.events.Topics;
import it.kristikomini.orders.domain.Order;
import it.kristikomini.orders.domain.OrderRepository;
import it.kristikomini.orders.domain.OrderService;
import it.kristikomini.orders.domain.OrderState;
import it.kristikomini.orders.messaging.EventJson;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Order-service saga legs over embedded Kafka + real Postgres:
 * <ul>
 *   <li>placing an order writes it PENDING and publishes {@code OrderPlaced} (via the outbox relay);</li>
 *   <li>a {@code ShipmentScheduled} event confirms the order;</li>
 *   <li>a {@code StockRejected} event cancels the order (compensation).</li>
 * </ul>
 * Skips locally without Docker (Postgres); runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1,
        topics = {Topics.ORDERS_PLACED, Topics.SHIPMENT_SCHEDULED, Topics.STOCK_REJECTED})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "outbox.relay.fixed-delay-ms=300"
})
class OrderSagaIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired EventJson json;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void placingAnOrderPublishesOrderPlaced() {
        Order order = orderService.place("SKU-1", 2, new BigDecimal("10.00"));
        assertThat(order.getState()).isEqualTo(OrderState.PENDING);

        Consumer<String, String> consumer = consumerFor(Topics.ORDERS_PLACED);
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(consumer, Topics.ORDERS_PLACED, Duration.ofSeconds(10));
        assertThat(record.value()).contains(order.getOrderId()).contains("SKU-1");
        consumer.close();
    }

    @Test
    void shipmentScheduledConfirmsTheOrder() {
        Order order = orderService.place("SKU-9", 1, new BigDecimal("5.00"));
        kafka.send(Topics.SHIPMENT_SCHEDULED, order.getOrderId(),
                json.toJson(new OrderEvents.ShipmentScheduled(order.getOrderId(), "TRK-123")));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orders.findByOrderId(order.getOrderId()).orElseThrow().getState())
                        .isEqualTo(OrderState.CONFIRMED));
    }

    @Test
    void stockRejectedCancelsTheOrder() {
        Order order = orderService.place("SKU-OUT", 1, new BigDecimal("5.00"));
        kafka.send(Topics.STOCK_REJECTED, order.getOrderId(),
                json.toJson(new OrderEvents.StockRejected(order.getOrderId(), "SKU-OUT", "OUT_OF_STOCK")));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orders.findByOrderId(order.getOrderId()).orElseThrow().getState())
                        .isEqualTo(OrderState.CANCELLED));
    }

    private Consumer<String, String> consumerFor(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("order-test-" + topic, "true", broker);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        c.subscribe(List.of(topic));
        return c;
    }
}
