package io.paramix.chat.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class AdmissionLimiterTest {

    @Test
    void reconnectStormIsFlattenedToTheConfiguredRate() throws Exception {
        AdmissionLimiter limiter = new AdmissionLimiter(100);
        long accepted = IntStream.range(0, 5_000).filter(i -> limiter.tryAcquire()).count();   // 5.000 istemci aynı anda
        assertThat(accepted).isBetween(100L, 105L);                                             // ilk anda sadece kova kadarı
        Thread.sleep(250);
        long later = IntStream.range(0, 5_000).filter(i -> limiter.tryAcquire()).count();
        assertThat(later).isBetween(20L, 30L);                                                  // 250 ms × 100/sn ≈ 25
    }

    @Test
    void retryAfterIsRandomizedSoRejectedClientsDoNotReturnTogether() {
        long distinct = IntStream.range(0, 200).map(i -> AdmissionLimiter.retryAfterSeconds()).distinct().count();
        assertThat(distinct).isGreaterThan(5);
        assertThat(IntStream.range(0, 200).map(i -> AdmissionLimiter.retryAfterSeconds())).allMatch(s -> s >= 1 && s <= 10);
    }
}
