package io.paylink.gateway.policy;

import io.paylink.gateway.limiter.BucketSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * Bir istek için hangi kovaların uygulanacağına karar verir (Adım 4 kararı).
 *
 * <p>Katmanlar ayrı ayrı değerlendirilir, her katmanda en spesifik kural seçilir (TENANT > PLAN):
 * <ol>
 *   <li><b>Genel kova</b> ({@code *}): tenant'ın toplam hızı</li>
 *   <li><b>Endpoint kovası</b>: pahalı endpoint'ler için ek, daha sıkı limit (varsa)</li>
 * </ol>
 * İstek tüm kovalardan jeton harcar; biri boşsa 429. Global kova ayrıca {@code RedisRateLimiter}'da.
 */
@Component
public class PolicyResolver {

    public Resolution resolve(ApiKeyRecord key, String method, String path, PolicySnapshot snapshot) {
        List<RateLimitPolicy> tenantRules = snapshot.policiesFor(ScopeType.TENANT, key.tenantId());
        List<RateLimitPolicy> planRules = snapshot.policiesFor(ScopeType.PLAN, key.plan());

        Optional<RateLimitPolicy> tenantGeneral = general(tenantRules);
        Optional<RateLimitPolicy> planGeneral = general(planRules);
        Optional<RateLimitPolicy> generalRule = tenantGeneral.or(() -> planGeneral);

        Optional<RateLimitPolicy> endpointRule = mostSpecificMatch(tenantRules, method, path)
                .or(() -> mostSpecificMatch(planRules, method, path));

        List<BucketSpec> buckets = new ArrayList<>(2);
        String tag = "{" + key.tenantId() + "}"; // Redis Cluster hash tag → tenant'ın tüm key'leri aynı slot'ta
        generalRule.ifPresent(r -> buckets.add(
                new BucketSpec("rl:" + tag + ":*", "general", r.ratePerSec(), r.burst())));
        endpointRule.ifPresent(r -> buckets.add(
                new BucketSpec("rl:" + tag + ":ep:" + r.endpoint().raw(), "endpoint", r.ratePerSec(), r.burst())));

        // Kota: tenant genel kuralında tanımlıysa o, değilse plan genel kuralındaki; ikisi de yoksa sınırsız.
        Long dailyQuota = tenantGeneral.map(RateLimitPolicy::dailyQuota)
                .or(() -> planGeneral.map(RateLimitPolicy::dailyQuota))
                .orElse(null);

        return new Resolution(key.tenantId(), key.plan(), List.copyOf(buckets), dailyQuota, tag);
    }

    private static Optional<RateLimitPolicy> general(List<RateLimitPolicy> rules) {
        return rules.stream().filter(RateLimitPolicy::isGeneral).findFirst();
    }

    private static Optional<RateLimitPolicy> mostSpecificMatch(List<RateLimitPolicy> rules, String method, String path) {
        return rules.stream()
                .filter(r -> !r.isGeneral())
                .filter(r -> r.endpoint().matches(method, path))
                .min((a, b) -> EndpointPattern.MOST_SPECIFIC_FIRST.compare(a.endpoint(), b.endpoint()));
    }

    /**
     * @param dailyQuota null = kota yok
     * @param hashTag    Redis Cluster hash tag'i, ör. {tenant_42}
     */
    public record Resolution(String tenantId, String plan, List<BucketSpec> buckets, Long dailyQuota, String hashTag) {

        public boolean hasRules() {
            return !buckets.isEmpty();
        }
    }
}
