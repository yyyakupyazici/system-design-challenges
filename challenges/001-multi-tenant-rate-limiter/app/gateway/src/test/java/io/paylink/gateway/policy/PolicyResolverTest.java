package io.paylink.gateway.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.paylink.gateway.limiter.BucketSpec;
import io.paylink.gateway.policy.PolicyResolver.Resolution;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolicyResolverTest {

    private final PolicyResolver resolver = new PolicyResolver();
    private long ids = 1;

    private RateLimitPolicy rule(ScopeType type, String scope, String endpoint, int rate, int burst, Long quota) {
        return new RateLimitPolicy(ids++, type, scope, EndpointPattern.parse(endpoint), rate, burst, quota, 1);
    }

    /** Challenge'daki örnek tablo (Adım 4). */
    private List<RateLimitPolicy> seed() {
        List<RateLimitPolicy> rules = new ArrayList<>();
        rules.add(rule(ScopeType.PLAN, "FREE", "*", 10, 10, 50_000L));
        rules.add(rule(ScopeType.PLAN, "PRO", "*", 200, 400, null));
        rules.add(rule(ScopeType.PLAN, "PRO", "POST /payments", 50, 100, null));
        rules.add(rule(ScopeType.TENANT, "tenant_42", "*", 500, 1000, null));
        return rules;
    }

    private Resolution resolve(List<RateLimitPolicy> rules, String tenant, String plan, String method, String path) {
        PolicySnapshot snapshot = PolicySnapshot.of(1, List.of(), rules);
        return resolver.resolve(new ApiKeyRecord("h", tenant, plan, true), method, path, snapshot);
    }

    @Test
    void proTenantPostPayments_getsGeneralAndEndpointBuckets() {
        Resolution r = resolve(seed(), "tenant_7", "PRO", "POST", "/payments");

        assertThat(r.buckets()).containsExactly(
                new BucketSpec("rl:{tenant_7}:*", "general", 200, 400),
                new BucketSpec("rl:{tenant_7}:ep:POST /payments", "endpoint", 50, 100));
        assertThat(r.dailyQuota()).isNull();
    }

    @Test
    void proTenantGet_onlyGeneralBucket() {
        Resolution r = resolve(seed(), "tenant_7", "PRO", "GET", "/payments/pl_123");

        assertThat(r.buckets()).extracting(BucketSpec::layer).containsExactly("general");
    }

    @Test
    void tenant42_tenantOverrideWinsForGeneral_planRuleStillAppliesToEndpoint() {
        // Yakup'un (c) cevabı + (d) ayrıntısı: her katman ayrı, her katmanda TENANT > PLAN
        Resolution r = resolve(seed(), "tenant_42", "PRO", "POST", "/payments");

        assertThat(r.buckets()).containsExactly(
                new BucketSpec("rl:{tenant_42}:*", "general", 500, 1000),
                new BucketSpec("rl:{tenant_42}:ep:POST /payments", "endpoint", 50, 100));
    }

    @Test
    void tenant42_withTenantSpecificEndpointRule_overridesPlanEndpointRule() {
        List<RateLimitPolicy> rules = seed();
        rules.add(rule(ScopeType.TENANT, "tenant_42", "POST /payments", 500, 1000, null));

        Resolution r = resolve(rules, "tenant_42", "PRO", "POST", "/payments");

        assertThat(r.buckets()).extracting(BucketSpec::ratePerSec).containsExactly(500, 500);
    }

    @Test
    void freePlan_hasDailyQuota_andTenantOverrideWithoutQuotaInheritsPlanQuota() {
        List<RateLimitPolicy> rules = seed();
        rules.add(rule(ScopeType.TENANT, "tenant_free_vip", "*", 20, 20, null));

        assertThat(resolve(rules, "tenant_free_1", "FREE", "GET", "/payments").dailyQuota()).isEqualTo(50_000L);
        Resolution vip = resolve(rules, "tenant_free_vip", "FREE", "GET", "/payments");
        assertThat(vip.dailyQuota()).isEqualTo(50_000L);
        assertThat(vip.buckets().getFirst().ratePerSec()).isEqualTo(20);
    }

    @Test
    void mostSpecificEndpointPatternWins() {
        List<RateLimitPolicy> rules = seed();
        rules.add(rule(ScopeType.PLAN, "PRO", "* /payments/**", 150, 300, null));
        rules.add(rule(ScopeType.PLAN, "PRO", "GET /payments/{id}", 100, 200, null));

        Resolution r = resolve(rules, "tenant_7", "PRO", "GET", "/payments/pl_1");

        assertThat(r.buckets().get(1).redisKey()).isEqualTo("rl:{tenant_7}:ep:GET /payments/{id}");
    }

    @Test
    void unknownPlan_hasNoRules() {
        assertThat(resolve(seed(), "t", "GOLD", "GET", "/payments").hasRules()).isFalse();
    }

    @Test
    void invalidEndpointExpression_isRejected() {
        assertThatThrownBy(() -> EndpointPattern.parse("payments")).isInstanceOf(IllegalArgumentException.class);
        assertThat(EndpointPattern.parse("post /payments").raw()).isEqualTo("POST /payments");
    }
}
