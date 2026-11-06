package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.redislab.atomic.CounterService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

class AtomicCounterIT extends AbstractRedisIT {

    @Autowired CounterService counters;
    @Autowired StringRedisTemplate redis;

    /** Kaybolan güncelleme, şansa bırakmadan: iki istemcinin adımlarını elle sıraya diziyoruz. */
    @Test
    void readModifyWriteLosesAnUpdateWhenTwoClientsInterleave() {
        String key = unique("counter");
        redis.opsForValue().set(key, "10");

        long seenByA = Long.parseLong(redis.opsForValue().get(key));   // A okur: 10
        long seenByB = Long.parseLong(redis.opsForValue().get(key));   // B okur: 10
        redis.opsForValue().set(key, String.valueOf(seenByA + 1));     // A yazar: 11
        redis.opsForValue().set(key, String.valueOf(seenByB + 1));     // B yazar: 11  ← A'nın artışı kayboldu

        assertThat(counters.get(key)).isEqualTo(11);                   // iki artış, ama 12 değil
    }

    @Test
    void incrNeverLosesAnUpdateUnderConcurrency() throws Exception {
        String key = unique("counter");
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            for (int i = 0; i < 1600; i++) {
                pool.submit(() -> counters.atomicIncrement(key));
            }
        }
        assertThat(counters.get(key)).isEqualTo(1600);
    }

    @Test
    void conditionalIncrementInLuaNeverExceedsTheLimit() throws Exception {
        String key = unique("seats");
        AtomicInteger granted = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            for (int i = 0; i < 200; i++) {
                pool.submit(() -> {
                    if (counters.incrementIfBelow(key, 50)) {
                        granted.incrementAndGet();
                    }
                });
            }
        }
        assertThat(granted.get()).isEqualTo(50);
        assertThat(counters.get(key)).isEqualTo(50);
    }
}
