package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.paramix.redislab.ratelimit.RateLimiters;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RateLimitIT extends AbstractRedisIT {

    @Autowired RateLimiters limiters;

    @Test
    void fixedWindowAllowsUpToTheLimit() {
        String c = unique("client");
        for (int i = 1; i <= 5; i++) {
            assertThat(limiters.fixedWindow(c, 5, Duration.ofMinutes(1)).allowed()).isTrue();
        }
        assertThat(limiters.fixedWindow(c, 5, Duration.ofMinutes(1)).allowed()).isFalse();
    }

    @Test
    void slidingLogForgetsRequestsOnceTheyLeaveTheWindow() throws Exception {
        String c = unique("client");
        for (int i = 0; i < 3; i++) {
            assertThat(limiters.slidingLog(c, 3, Duration.ofMillis(500)).allowed()).isTrue();
        }
        assertThat(limiters.slidingLog(c, 3, Duration.ofMillis(500)).allowed()).isFalse();
        Thread.sleep(600);
        assertThat(limiters.slidingLog(c, 3, Duration.ofMillis(500)).allowed()).isTrue();
    }

    @Test
    void tokenBucketAllowsABurstThenRefillsAtTheRate() throws Exception {
        String c = unique("client");
        for (int i = 0; i < 5; i++) {
            assertThat(limiters.tokenBucket(c, 5, 10).allowed()).isTrue();   // kova dolu: 5'lik patlama
        }
        assertThat(limiters.tokenBucket(c, 5, 10).allowed()).isFalse();
        Thread.sleep(250);                                                  // 10 jeton/sn → ~2 jeton
        assertThat(limiters.tokenBucket(c, 5, 10).allowed()).isTrue();
    }
}
