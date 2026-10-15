package io.paylink.gateway.limiter;

import static org.assertj.core.api.Assertions.assertThat;

import io.paylink.gateway.config.RateLimitProperties;
import io.paylink.gateway.it.TestInfra;
import io.paylink.gateway.policy.PolicyResolver.Resolution;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/** Lua script'ini gerçek Redis'e karşı test eder (race condition, all-or-nothing, kota, burst değişimi). */
@EnabledIf("io.paylink.gateway.it.TestInfra#available")
class TokenBucketScriptIT {

    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;
    private static RedisRateLimiter limiter;
    private static final LocalDate TODAY = LocalDate.of(2026, 10, 15);

    @BeforeAll
    static void setUp() {
        factory = new LettuceConnectionFactory(TestInfra.redisHost(), TestInfra.redisPort());
        factory.afterPropertiesSet();
        factory.start();
        redis = new ReactiveStringRedisTemplate(factory);
        RateLimitProperties props = new RateLimitProperties(true, Duration.ofSeconds(2), Duration.ofDays(2),
                Duration.ofSeconds(60), new RateLimitProperties.Global(true, 5, 5));
        limiter = new RedisRateLimiter(redis, props);
    }

    @AfterAll
    static void tearDown() {
        factory.destroy();
    }

    private static String tenant() {
        return "t-" + UUID.randomUUID();
    }

    private static Resolution resolution(String tenant, Long quota, BucketSpec... buckets) {
        return new Resolution(tenant, "PRO", List.of(buckets), quota, "{" + tenant + "}");
    }

    private static RateLimitDecision call(Resolution r) {
        return limiter.checkTenant(r, TODAY).block();
    }

    @Test
    void burstIsCapacity_slowRefill() {
        String t = tenant();
        Resolution r = resolution(t, null, new BucketSpec("rl:{" + t + "}:*", "general", 1, 5));

        List<Boolean> results = IntStream.range(0, 7).mapToObj(i -> call(r).allowed()).toList();

        assertThat(results).containsExactly(true, true, true, true, true, false, false);
        RateLimitDecision rejected = call(r);
        assertThat(rejected.reason()).isEqualTo(RateLimitDecision.Reason.RATE);
        assertThat(rejected.retryAfterMs()).isBetween(1L, 1000L);
    }

    @Test
    void concurrentPods_doNotOverspend_noLostUpdate() throws Exception {
        // 10 "pod" aynı anda 200 istek atıyor; kova 50. Lua atomik olduğu için en fazla ~50 geçmeli.
        String t = tenant();
        Resolution r = resolution(t, null, new BucketSpec("rl:{" + t + "}:*", "general", 1, 50));
        ConcurrentLinkedQueue<Boolean> results = new ConcurrentLinkedQueue<>();
        try (ExecutorService pods = Executors.newFixedThreadPool(10)) {
            for (int i = 0; i < 200; i++) {
                pods.submit(() -> results.add(call(r).allowed()));
            }
        }
        long allowed = results.stream().filter(b -> b).count();
        assertThat(allowed).isBetween(50L, 52L); // test süresince en fazla 1-2 jeton dolabilir
    }

    @Test
    void allOrNothing_rejectedRequestDoesNotSpendGeneralBucket() {
        String t = tenant();
        BucketSpec general = new BucketSpec("rl:{" + t + "}:*", "general", 1, 10);
        BucketSpec payments = new BucketSpec("rl:{" + t + "}:ep:POST /payments", "endpoint", 1, 2);
        Resolution r = resolution(t, null, general, payments);

        assertThat(call(r).allowed()).isTrue();
        assertThat(call(r).allowed()).isTrue();
        RateLimitDecision third = call(r);

        assertThat(third.allowed()).isFalse();
        assertThat(third.limit()).isEqualTo(1); // darboğaz: endpoint kovası
        double generalTokens = Double.parseDouble(redis.opsForHash().get(general.redisKey(), "tokens").block().toString());
        assertThat(generalTokens).isLessThan(9.0).isGreaterThanOrEqualTo(8.0); // 10 - 2, üçüncü istek harcamadı
    }

    @Test
    void dailyQuota_countsOnlyAcceptedRequests_andRefundWorks() {
        String t = tenant();
        Resolution r = resolution(t, 3L, new BucketSpec("rl:{" + t + "}:*", "general", 100, 100));

        assertThat(call(r).quotaRemaining()).isEqualTo(2);
        assertThat(call(r).quotaRemaining()).isEqualTo(1);
        RateLimitDecision third = call(r);
        assertThat(third.quotaRemaining()).isZero();
        RateLimitDecision fourth = call(r);
        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.reason()).isEqualTo(RateLimitDecision.Reason.QUOTA);

        // 5xx alan istek iade edilir → bir hak geri gelir
        limiter.refundQuota(third).block();
        assertThat(call(r).allowed()).isTrue();

        String quotaKey = "quota:{" + t + "}:" + TODAY;
        assertThat(redis.getExpire(quotaKey).block()).isPositive();
    }

    @Test
    void lowerBurst_clampsExistingTokensImmediately() {
        String t = tenant();
        String key = "rl:{" + t + "}:*";
        call(resolution(t, null, new BucketSpec(key, "general", 200, 400)));

        RateLimitDecision afterChange = call(resolution(t, null, new BucketSpec(key, "general", 200, 100)));

        assertThat(afterChange.remaining()).isEqualTo(99); // 400 değil, yeni kapasite 100'den 1 düştü
    }

    @Test
    void globalBucket_rejectsWith503Reason() {
        redis.delete("rl:{global}:all").block();
        List<RateLimitDecision> decisions = IntStream.range(0, 7).mapToObj(i -> limiter.checkGlobal().block()).toList();

        assertThat(decisions).filteredOn(d -> !d.allowed()).isNotEmpty()
                .allMatch(d -> d.reason() == RateLimitDecision.Reason.GLOBAL);
    }
}
