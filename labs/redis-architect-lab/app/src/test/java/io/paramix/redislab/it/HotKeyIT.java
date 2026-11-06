package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.paramix.redislab.common.KeySlot;
import io.paramix.redislab.hotkey.HotReadCache;
import io.paramix.redislab.hotkey.ShardedCounter;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

class HotKeyIT extends AbstractRedisIT {

    @Autowired ShardedCounter counter;
    @Autowired HotReadCache hotRead;
    @Autowired StringRedisTemplate redis;

    @Test
    void shardedCounterSpreadsWritesAcrossSlotsAndStillSumsCorrectly() throws Exception {
        String name = unique("begeni");
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            for (int i = 0; i < 2000; i++) {
                pool.submit(() -> counter.increment(name));
            }
        }
        assertThat(counter.total(name)).isEqualTo(2000);
        long distinctSlots = counter.shardKeys(name).stream().map(KeySlot::of).distinct().count();
        assertThat(distinctSlots).isGreaterThan(1);           // parçalar farklı slotlarda → farklı düğümlere dağılabilir
    }

    @Test
    void shortLocalCacheShieldsRedisFromAHotRead() {
        String key = unique("kampanya-config");
        redis.opsForValue().set(key, "{\"indirim\":20}");
        long before = hotRead.redisReads();
        for (int i = 0; i < 1000; i++) {
            assertThat(hotRead.get(key, Duration.ofSeconds(5))).contains("indirim");
        }
        assertThat(hotRead.redisReads() - before).isEqualTo(1);
    }
}
