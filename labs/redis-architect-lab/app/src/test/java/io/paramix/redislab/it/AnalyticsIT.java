package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.paramix.redislab.probabilistic.AnalyticsService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

class AnalyticsIT extends AbstractRedisIT {

    @Autowired AnalyticsService analytics;
    @Autowired StringRedisTemplate redis;

    @Test
    void hyperLogLogCountsUniquesWithin2PercentInAFewKilobytes() {
        String page = unique("kampanya");
        for (int i = 0; i < 20_000; i++) {
            analytics.visit(page, "2026-11-06", "visitor-" + (i % 10_000));   // her ziyaretçi 2 kez
        }
        long n = analytics.uniqueVisitors(page, "2026-11-06");
        assertThat(n).isBetween(9_800L, 10_200L);
        Long bytes = redis.execute((org.springframework.data.redis.core.RedisCallback<Long>) c ->
                c.stringCommands().strLen(("uv:{" + page + "}:2026-11-06").getBytes()));
        assertThat(bytes).isLessThanOrEqualTo(12 * 1024 + 16);              // 10 bin kişi için ≤ 12 KB

        for (int i = 5_000; i < 15_000; i++) {
            analytics.visit(page, "2026-11-07", "visitor-" + i);             // 5 bini dünkülerle aynı
        }
        assertThat(analytics.uniqueVisitorsOver(page, List.of("2026-11-06", "2026-11-07"))).isBetween(14_700L, 15_300L);
    }

    @Test
    void bitmapDailyActiveAndRetention() {
        String app = unique("cuzdan");
        for (long u : new long[] {1, 5, 42, 1_000_000}) {
            analytics.markActive(app, "2026-11-05", u);
        }
        for (long u : new long[] {5, 42, 77}) {
            analytics.markActive(app, "2026-11-06", u);
        }
        assertThat(analytics.wasActive(app, "2026-11-05", 42)).isTrue();
        assertThat(analytics.wasActive(app, "2026-11-05", 77)).isFalse();
        assertThat(analytics.activeCount(app, "2026-11-05")).isEqualTo(4);
        assertThat(analytics.activeOnBoth(app, "2026-11-05", "2026-11-06")).isEqualTo(2);   // 5 ve 42
    }

    @Test
    void nearestCouriersAreSortedByDistance() {
        String city = unique("istanbul");
        analytics.updateCourier(city, "kadikoy-kurye", 29.0270, 40.9903);
        analytics.updateCourier(city, "moda-kurye", 29.0260, 40.9830);
        analytics.updateCourier(city, "uskudar-kurye", 29.0150, 41.0260);
        analytics.updateCourier(city, "taksim-kurye", 28.9850, 41.0370);
        // Kadıköy iskelesinden 3 km içinde, yakından uzağa
        assertThat(analytics.nearestCouriers(city, 29.0230, 40.9920, 3, 5)).containsExactly("kadikoy-kurye", "moda-kurye");
    }
}
