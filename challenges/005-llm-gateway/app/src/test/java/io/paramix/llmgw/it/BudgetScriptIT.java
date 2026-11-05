package io.paramix.llmgw.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.llmgw.budget.BudgetService;
import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.LimitProperties;
import io.paramix.llmgw.config.GatewayProperties.OverBudget;
import io.paramix.llmgw.config.GatewayProperties.TeamProperties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Bütçe Lua script'leri gerçek Redis'te: 10 kişilik ekip, kişi başı 60 kredi garanti + 600 kredi havuz, tavan 300.
 */
@EnabledIf("io.paramix.llmgw.it.TestInfra#redisAvailable")
class BudgetScriptIT {

    static final long M = 1_000_000;   // 1 kredi
    StringRedisTemplate redis;
    BudgetService budget;

    @BeforeEach
    void setUp() {
        LettuceConnectionFactory cf = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(TestInfra.redisHost(), TestInfra.redisPort()));
        cf.afterPropertiesSet();
        redis = new StringRedisTemplate(cf);
        redis.afterPropertiesSet();
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        TeamProperties team = new TeamProperties(1200, OverBudget.REJECT, 200000,
                List.of("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "u9", "u10"), 60, 300);
        GatewayProperties props = new GatewayProperties(Map.of(), Map.of("support", team), Map.of(), Map.of(), null, null,
                new LimitProperties(32000, Duration.ofMinutes(10), "Europe/Istanbul"), null);
        budget = new BudgetService(redis, props, Clock.systemUTC(), new SimpleMeterRegistry());
    }

    @Test
    void guaranteeIsUsedFirstThenThePoolAndTheUserCeilingHolds() {
        // u1: 250 kredi → 60 garantiden + 190 havuzdan
        BudgetService.Outcome a = budget.reserve("support", "u1", "r1", 250 * M);
        assertThat(a.granted()).isTrue();
        // u1 tavanı 300: 60 daha istenirse 310 olur → reddedilir (kişi tavanı)
        BudgetService.Outcome b = budget.reserve("support", "u1", "r2", 60 * M);
        assertThat(b.denial()).isEqualTo(BudgetService.Denial.USER_CEILING);

        // Diğerleri havuzu bitirse bile u10'un 60 kredilik garantisi yerinde durur
        budget.reserve("support", "u2", "r3", 300 * M);   // 60 + 240 havuz → havuzda 600-190-240 = 170 kaldı
        budget.reserve("support", "u3", "r4", 230 * M);   // 60 + 170 havuz → havuz bitti
        assertThat(budget.reserve("support", "u4", "r5", 61 * M).denial()).isEqualTo(BudgetService.Denial.TEAM_POOL);
        assertThat(budget.reserve("support", "u10", "r6", 60 * M).granted()).isTrue();
        // Üye olmayanın garantisi yok, havuz bitti → red
        assertThat(budget.reserve("support", "stranger", "r7", 1 * M).denial()).isEqualTo(BudgetService.Denial.TEAM_POOL);
    }

    @Test
    void settleRefundsTheDifferenceToThePoolFirstAndIsIdempotent() {
        BudgetService.Outcome a = budget.reserve("support", "u1", "r1", 100 * M);   // 60 garanti + 40 havuz
        budget.settle(a.reservation(), 30 * M);                                     // 70 iade: 40 havuza, 30 garantiye
        budget.settle(a.reservation(), 30 * M);                                     // tekrar: etkisiz

        Map<String, Object> s = budget.status("support", "u1");
        assertThat(s.get("spentCredits")).isEqualTo(30.0);
        assertThat(s.get("reservedCredits")).isEqualTo(0.0);
        assertThat(s.get("poolUsedCredits")).isEqualTo(0.0);
        @SuppressWarnings("unchecked") Map<String, Object> u = (Map<String, Object>) s.get("user");
        assertThat(u.get("guaranteeUsedCredits")).isEqualTo(30.0);
    }

    @Test
    void retryOfTheSameRequestDoesNotReserveTwice() {
        budget.reserve("support", "u1", "same", 100 * M);
        budget.reserve("support", "u1", "same", 100 * M);
        @SuppressWarnings("unchecked") Map<String, Object> u = (Map<String, Object>) budget.status("support", "u1").get("user");
        assertThat(u.get("guaranteeUsedCredits")).isEqualTo(60.0);
        assertThat(u.get("poolUsedCredits")).isEqualTo(40.0);
    }

    @Test
    void fiftyConcurrentStreamsCannotOverspendTheTeam() throws Exception {
        // Üye olmayan 50 kullanıcı aynı anda 20'şer kredi ister: havuz 600 → en fazla 30 kabul
        AtomicInteger granted = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<?>> fs = new java.util.ArrayList<>();
            for (int i = 0; i < 50; i++) {
                int n = i;
                fs.add(pool.submit(() -> {
                    if (budget.reserve("support", "guest" + n, "c" + n, 20 * M).granted()) {
                        granted.incrementAndGet();
                    }
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
        }
        assertThat(granted.get()).isEqualTo(30);
        assertThat(budget.status("support", "x").get("poolUsedCredits")).isEqualTo(600.0);
    }

    @Test
    void reaperChargesTheFullReservationWhenSettleNeverComes() {
        BudgetService expired = new BudgetService(redis, new GatewayProperties(Map.of(),
                Map.of("support", new TeamProperties(1200, OverBudget.REJECT, 200000, List.of(), 0, 0)), Map.of(), Map.of(),
                null, null, new LimitProperties(32000, Duration.ofMillis(-1), "Europe/Istanbul"), null),
                Clock.systemUTC(), new SimpleMeterRegistry());
        expired.reserve("support", "u1", "dead-pod", 5 * M);
        expired.reapExpired();
        assertThat(expired.status("support", "u1").get("spentCredits")).isEqualTo(5.0);
        assertThat(expired.status("support", "u1").get("reservedCredits")).isEqualTo(0.0);
    }
}
