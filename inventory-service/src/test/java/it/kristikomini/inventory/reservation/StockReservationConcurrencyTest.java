package it.kristikomini.inventory.reservation;

import it.kristikomini.inventory.domain.Stock;
import it.kristikomini.inventory.domain.StockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The concurrency shootout: fire 100 requests at the <b>last single unit</b> and prove that exactly
 * one wins and stock never goes negative — for each reservation strategy. Uses a
 * {@link CountDownLatch} start gate plus a {@link CyclicBarrier} so the threads genuinely collide
 * rather than trickle through. Real PostgreSQL (Testcontainers); skips without Docker, runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class StockReservationConcurrencyTest {

    private static final String SKU = "SKU-1";
    private static final int THREADS = 100;

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    Map<String, StockReserver> reservers; // keyed by bean name: "optimistic", "pessimistic"

    @Autowired
    StockRepository stock;

    @BeforeEach
    void seedOneUnit() {
        stock.deleteAll();
        stock.save(new Stock(SKU, 1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void exactlyOneReservationWinsForTheLastUnit(String strategyName) throws Exception {
        StockReserver reserver = reservers.get(strategyName);
        assertThat(reserver).as("strategy bean %s", strategyName).isNotNull();

        // One virtual thread per request → all 100 run at once; the barrier releases them together.
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier allReady = new CyclicBarrier(THREADS);
        AtomicInteger reserved = new AtomicInteger();

        List<Callable<Void>> tasks = IntStream.range(0, THREADS)
                .<Callable<Void>>mapToObj(i -> () -> {
                    allReady.await();   // block until all 100 have arrived, then fire together
                    if (reserver.reserve(SKU, 1) == StockReserver.ReservationResult.RESERVED) {
                        reserved.incrementAndGet();
                    }
                    return null;
                })
                .toList();

        List<Future<Void>> futures = tasks.stream().map(pool::submit).toList();
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(reserved.get()).as("exactly one reservation succeeds (%s)", strategyName).isEqualTo(1);
        assertThat(stock.findBySku(SKU)).get()
                .extracting(Stock::getAvailable).as("no oversell (%s)", strategyName).isEqualTo(0);
    }
}
