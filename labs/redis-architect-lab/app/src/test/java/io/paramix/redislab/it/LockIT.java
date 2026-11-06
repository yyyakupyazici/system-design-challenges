package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.redislab.lock.FencedStore;
import io.paramix.redislab.lock.LockHandle;
import io.paramix.redislab.lock.RedisLock;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class LockIT extends AbstractRedisIT {

    @Autowired RedisLock locks;
    @Autowired FencedStore store;

    @Test
    void onlyOneOwnerAtATime() {
        String res = unique("report");
        LockHandle a = locks.tryAcquire(res, Duration.ofSeconds(10)).orElseThrow();
        assertThat(locks.tryAcquire(res, Duration.ofSeconds(10))).isEmpty();
        assertThat(locks.release(a)).isTrue();
        assertThat(locks.tryAcquire(res, Duration.ofSeconds(10))).isPresent();
    }

    @Test
    void someoneElsesTokenCannotReleaseTheLock() {
        String res = unique("report");
        LockHandle a = locks.tryAcquire(res, Duration.ofSeconds(10)).orElseThrow();
        LockHandle impostor = new LockHandle(res, "baska-token", a.fencingToken());
        assertThat(locks.release(impostor)).isFalse();
        assertThat(locks.tryAcquire(res, Duration.ofSeconds(10))).isEmpty();   // kilit hâlâ A'da
    }

    @Test
    void expiredLockGoesToANewOwnerWithAHigherFencingToken() throws Exception {
        String res = unique("job");
        LockHandle a = locks.tryAcquire(res, Duration.ofMillis(150)).orElseThrow();
        Thread.sleep(300);                                                      // A "GC'de dondu", kilidin süresi doldu
        LockHandle b = locks.tryAcquire(res, Duration.ofSeconds(10)).orElseThrow();
        assertThat(b.fencingToken()).isGreaterThan(a.fencingToken());
        assertThat(locks.release(a)).isFalse();                                // A uyanınca B'nin kilidini silemez
        assertThat(locks.extend(a, Duration.ofSeconds(5))).isFalse();
        assertThat(locks.extend(b, Duration.ofSeconds(5))).isTrue();
    }

    /** Kilit tek başına yetmez: zombi sahip, yeni sahipten SONRA yazmaya çalışırsa depolama onu reddetmeli. */
    @Test
    void fencingTokenStopsTheZombieOwnersLateWrite() throws Exception {
        String res = unique("account");
        LockHandle zombie = locks.tryAcquire(res, Duration.ofMillis(100)).orElseThrow();
        Thread.sleep(200);
        LockHandle fresh = locks.tryAcquire(res, Duration.ofSeconds(10)).orElseThrow();

        assertThat(store.write(res, fresh.fencingToken(), "yeni sahibin değeri")).isTrue();
        assertThat(store.write(res, zombie.fencingToken(), "zombinin geç yazması")).isFalse();
        assertThat(store.read(res)).isEqualTo("yeni sahibin değeri");
    }

    @Test
    void withLockNeverRunsTheCriticalSectionTwiceAtOnce() throws Exception {
        String res = unique("cron");
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        AtomicInteger ran = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(20)) {
            for (int i = 0; i < 20; i++) {
                pool.submit(() -> locks.withLock(res, Duration.ofSeconds(5), h -> {
                    maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
                    ran.incrementAndGet();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    inside.decrementAndGet();
                    return null;
                }));
            }
        }
        assertThat(maxInside.get()).isEqualTo(1);
        assertThat(ran.get()).isBetween(1, 20);
    }
}
