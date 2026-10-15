package io.paylink.gateway.policy;

/**
 * Bir limit kuralı (DB'deki rate_limit_policy satırı).
 *
 * @param dailyQuota null ise "tanımsız": daha genel kuraldan devralınır, hiçbir yerde yoksa sınırsız.
 */
public record RateLimitPolicy(
        long id,
        ScopeType scopeType,
        String scopeId,
        EndpointPattern endpoint,
        int ratePerSec,
        int burst,
        Long dailyQuota,
        long version) {

    public boolean isGeneral() {
        return endpoint.isAny();
    }
}
