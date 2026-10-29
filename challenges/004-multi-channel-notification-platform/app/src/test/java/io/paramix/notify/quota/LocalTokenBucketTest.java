package io.paramix.notify.quota;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class LocalTokenBucketTest {

    @Test
    void burstUpToRate_thenWait_thenRefill() {
        AtomicLong nanos = new AtomicLong();
        LocalTokenBucket bucket = new LocalTokenBucket(10, nanos::get);   // 10/sn, kapasite 10

        for (int i = 0; i < 10; i++) {
            assertThat(bucket.tryAcquire()).isZero();
        }
        assertThat(bucket.tryAcquire()).isEqualTo(100);   // bir jeton 100 ms'de dolar

        nanos.addAndGet(100_000_000L);
        assertThat(bucket.tryAcquire()).isZero();
        assertThat(bucket.tryAcquire()).isPositive();
    }
}
