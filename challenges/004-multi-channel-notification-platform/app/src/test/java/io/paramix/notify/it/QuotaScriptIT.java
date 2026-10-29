package io.paramix.notify.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.config.ProviderProperties;
import io.paramix.notify.config.ProviderProperties.Share;
import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.quota.AimdController;
import io.paramix.notify.quota.QuotaDecision;
import io.paramix.notify.quota.QuotaService;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Öncelikli kota Lua script'i, gerçek Redis'e karşı.
 * Test sağlayıcısı: global 100/sn; garanti critical 20, transactional 20 (Σ 40 → paylaşılan havuz 60);
 * tavan critical 80, transactional 80, marketing 60.
 */
@EnabledIf("io.paramix.notify.it.TestInfra#redisAvailable")
class QuotaScriptIT {

    private static final String P = "itq";
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static NotifyProperties props;
    private static QuotaService quota;

    @BeforeAll
    static void setUp() {
        factory = connection(TestInfra.redisHost(), TestInfra.redisPort());
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
        props = properties();
        quota = new QuotaService(redis, props, new SimpleMeterRegistry());
    }

    @AfterAll
    static void tearDown() {
        factory.destroy();
    }

    @BeforeEach
    void clean() {
        redis.delete(redis.keys("q:{" + P + "}:*"));
    }

    /*
     * Üst sınırlar ölçüm süresine göre hesaplanır: istekler atılırken geçen her milisaniyede kovalar
     * saniyedeki hızları oranında doluyor (kova kapasitesi + hız × geçen süre).
     */

    @Test
    void marketingBurst_cannotEatOtherPrioritiesGuarantees() {
        Burst marketing = burst(Priority.MARKETING, 300);
        Burst critical = burst(Priority.CRITICAL, 100);
        Burst transactional = burst(Priority.TRANSACTIONAL, 100);

        assertThat(marketing.allowed()).isBetween(60, marketing.upper(60, 60));
        assertThat(critical.allowed()).as("critical garantisi korunur").isGreaterThanOrEqualTo(20);
        assertThat(transactional.allowed())
                .as("transactional garantisi korunur (marketing ve critical birlikte ödünç alsa bile)")
                .isGreaterThanOrEqualTo(20);
    }

    @Test
    void quietNight_criticalAloneCanBorrowUpToItsCeiling() {
        Burst critical = burst(Priority.CRITICAL, 300);
        assertThat(critical.allowed()).isBetween(80, critical.upper(80, 80));
    }

    @Test
    void totalNeverExceedsGlobalRate() {
        long start = System.nanoTime();
        int total = burst(Priority.MARKETING, 300).allowed() + burst(Priority.CRITICAL, 300).allowed()
                + burst(Priority.TRANSACTIONAL, 300).allowed();
        Burst all = new Burst(total, (System.nanoTime() - start) / 1_000_000);
        assertThat(total).isBetween(100, all.upper(100, 100));
    }

    @Test
    void denied_tellsHowLongToWait() {
        burst(Priority.MARKETING, 300);
        QuotaDecision d = quota.tryAcquire(P, Priority.MARKETING);
        assertThat(d.allowed()).isFalse();
        assertThat(d.waitMs()).isBetween(1L, 1000L);
    }

    @Test
    void acquireWithin_waitsForRefillInsteadOfFailing() {
        burst(Priority.MARKETING, 300);
        QuotaDecision d = quota.acquireWithin(P, Priority.MARKETING, Duration.ofMillis(500));
        assertThat(d.allowed()).isTrue();
    }

    @Test
    void aimd_429ShrinksAllBucketsProportionally() {
        AimdController aimd = new AimdController(redis, props, new SimpleMeterRegistry());
        aimd.onRateLimited(P);                               // 100 → 70

        assertThat(aimd.effectiveRate(P)).isEqualTo(70);
        Burst marketing = burst(Priority.MARKETING, 300);
        assertThat(marketing.allowed()).isBetween(42, marketing.upper(42, 42));   // 60 × 0.7
    }

    @Test
    void redisDown_criticalUsesLocalBucket_marketingStops() {
        LettuceConnectionFactory dead = connection("127.0.0.1", 1);   // kimse dinlemiyor
        try {
            StringRedisTemplate deadRedis = new StringRedisTemplate(dead);
            deadRedis.afterPropertiesSet();
            QuotaService degraded = new QuotaService(deadRedis, props, new SimpleMeterRegistry());

            QuotaDecision critical = degraded.tryAcquire(P, Priority.CRITICAL);
            QuotaDecision marketing = degraded.tryAcquire(P, Priority.MARKETING);

            assertThat(critical.allowed()).isTrue();
            assertThat(critical.source()).isEqualTo(QuotaDecision.Source.LOCAL_FALLBACK);
            assertThat(marketing.allowed()).as("fail-closed").isFalse();
        } finally {
            dead.destroy();
        }
    }

    private record Burst(int allowed, long elapsedMs) {
        /** kapasite + saniyedeki hız × geçen süre (+1 yuvarlama payı) */
        int upper(int capacity, int ratePerSec) {
            return capacity + (int) Math.ceil(ratePerSec * elapsedMs / 1000.0) + 1;
        }
    }

    private static Burst burst(Priority priority, int n) {
        long start = System.nanoTime();
        int allowed = 0;
        for (int i = 0; i < n; i++) {
            if (quota.tryAcquire(P, priority).allowed()) {
                allowed++;
            }
        }
        return new Burst(allowed, (System.nanoTime() - start) / 1_000_000);
    }

    private static LettuceConnectionFactory connection(String host, int port) {
        LettuceConnectionFactory f = new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofMillis(300)).build());
        f.afterPropertiesSet();
        return f;
    }

    private static NotifyProperties properties() {
        ProviderProperties provider = new ProviderProperties(Channel.SMS, 1, 100, Map.of(
                Priority.CRITICAL, new Share(20, 80),
                Priority.TRANSACTIONAL, new Share(20, 80),
                Priority.MARKETING, new Share(0, 60)), true);
        return new NotifyProperties(
                new NotifyProperties.Roles(true, true, true, true),
                new NotifyProperties.Kafka(1, (short) 1),
                new NotifyProperties.Worker(List.of(Channel.SMS), null),
                new NotifyProperties.Delivery(Duration.ofSeconds(60), Duration.ofSeconds(10), 5, Duration.ofSeconds(30)),
                new NotifyProperties.Quota(2, Duration.ofSeconds(60), 0.7, 50, Duration.ofSeconds(30)),
                Map.of(P, provider));
    }
}
