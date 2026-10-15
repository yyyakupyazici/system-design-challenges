package io.paylink.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import io.github.resilience4j.bulkhead.BulkheadFullException;

/** Son sigorta: aynı anda en fazla 2 çağrı "DB'ye" gider, fazlası anında reddedilir. */
@SpringBootTest(properties = {
        "paylink.simulated-db-latency=300ms",
        "resilience4j.bulkhead.instances.paymentDb.max-concurrent-calls=2"
})
class BulkheadTest {

    @Autowired
    PaymentService service;

    @Test
    void excessConcurrentCalls_areRejectedImmediately() throws Exception {
        CreatePaymentRequest req = new CreatePaymentRequest(new BigDecimal("10"), "TRY", "test");
        List<Future<?>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 6; i++) {
                futures.add(pool.submit(() -> service.create("t", req)));
            }
        }
        long rejected = futures.stream().filter(f -> {
            try {
                f.get();
                return false;
            } catch (Exception e) {
                return e.getCause() instanceof BulkheadFullException;
            }
        }).count();

        assertThat(rejected).isEqualTo(4);
    }
}
