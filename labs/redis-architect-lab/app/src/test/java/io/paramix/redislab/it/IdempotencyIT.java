package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.paramix.redislab.idempotency.IdempotencyService;
import io.paramix.redislab.idempotency.IdempotencyService.Executed;
import io.paramix.redislab.idempotency.IdempotencyService.Outcome;
import io.paramix.redislab.idempotency.IdempotencyService.Replayed;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class IdempotencyIT extends AbstractRedisIT {

    @Autowired IdempotencyService idem;

    @Test
    void aRetryReturnsTheFirstResultWithoutRunningAgain() {
        String key = unique("pay");
        AtomicInteger runs = new AtomicInteger();
        Outcome first = idem.execute(key, Duration.ofMinutes(1), () -> "odeme-" + runs.incrementAndGet());
        Outcome retry = idem.execute(key, Duration.ofMinutes(1), () -> "odeme-" + runs.incrementAndGet());
        assertThat(first).isEqualTo(new Executed("odeme-1"));
        assertThat(retry).isEqualTo(new Replayed("odeme-1"));
        assertThat(runs.get()).isEqualTo(1);
    }

    @Test
    void twentyConcurrentDuplicatesRunTheActionOnce() throws Exception {
        String key = unique("pay");
        AtomicInteger runs = new AtomicInteger();
        List<Callable<Outcome>> calls = IntStream.range(0, 20).<Callable<Outcome>>mapToObj(i -> () ->
                idem.execute(key, Duration.ofMinutes(1), () -> {
                    runs.incrementAndGet();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "ok";
                })).toList();
        try (ExecutorService pool = Executors.newFixedThreadPool(20)) {
            long executed = pool.invokeAll(calls).stream().filter(f -> {
                try {
                    return f.get() instanceof Executed;
                } catch (Exception e) {
                    return false;
                }
            }).count();
            assertThat(executed).isEqualTo(1);
        }
        assertThat(runs.get()).isEqualTo(1);
    }

    @Test
    void aFailedAttemptCanBeRetried() {
        String key = unique("pay");
        assertThatThrownBy(() -> idem.execute(key, Duration.ofMinutes(1), () -> {
            throw new IllegalStateException("banka zaman aşımı");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(idem.execute(key, Duration.ofMinutes(1), () -> "ikinci deneme")).isEqualTo(new Executed("ikinci deneme"));
    }
}
