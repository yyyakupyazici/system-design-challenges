package io.paylink.gateway.policy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kuralların ve API key rehberinin bellekteki değişmez (immutable) kopyası.
 * İstek yolunda sadece bu okunur → ağ çağrısı yok, nanosaniye mertebesinde.
 * Yenileme sırasında yeni bir snapshot oluşturulup atomik olarak değiştirilir.
 */
public final class PolicySnapshot {

    private static final PolicySnapshot EMPTY = new PolicySnapshot(-1, Map.of(), Map.of());

    private final long version;
    private final Map<String, ApiKeyRecord> apiKeysByHash;
    private final Map<String, List<RateLimitPolicy>> policiesByScope;

    private PolicySnapshot(long version,
                           Map<String, ApiKeyRecord> apiKeysByHash,
                           Map<String, List<RateLimitPolicy>> policiesByScope) {
        this.version = version;
        this.apiKeysByHash = apiKeysByHash;
        this.policiesByScope = policiesByScope;
    }

    public static PolicySnapshot empty() {
        return EMPTY;
    }

    public static PolicySnapshot of(long version, Collection<ApiKeyRecord> apiKeys, Collection<RateLimitPolicy> policies) {
        Map<String, ApiKeyRecord> keys = apiKeys.stream()
                .collect(Collectors.toUnmodifiableMap(ApiKeyRecord::keyHash, k -> k));
        Map<String, List<RateLimitPolicy>> byScope = policies.stream()
                .collect(Collectors.groupingBy(p -> scopeKey(p.scopeType(), p.scopeId()),
                        Collectors.toUnmodifiableList()));
        return new PolicySnapshot(version, keys, Map.copyOf(byScope));
    }

    private static String scopeKey(ScopeType type, String id) {
        return type.name() + ":" + id;
    }

    public boolean isLoaded() {
        return version >= 0;
    }

    public long version() {
        return version;
    }

    public ApiKeyRecord apiKey(String keyHash) {
        return apiKeysByHash.get(keyHash);
    }

    public List<RateLimitPolicy> policiesFor(ScopeType type, String scopeId) {
        return policiesByScope.getOrDefault(scopeKey(type, scopeId), List.of());
    }

    public List<RateLimitPolicy> allPolicies() {
        return policiesByScope.values().stream().flatMap(List::stream).toList();
    }

    public int apiKeyCount() {
        return apiKeysByHash.size();
    }
}
