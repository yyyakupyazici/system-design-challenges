package io.paramix.llmgw.cache;

/**
 * @param exact birebir aynı istek için yanıt cache'i
 * @param semantic anlamca benzer soru için yanıt cache'i (daha riskli, daha sıkı kurallı)
 * @param reason kapalıysa neden (log/metrik için)
 */
public record CacheDecision(boolean exact, boolean semantic, String reason) {

    public static CacheDecision none(String reason) {
        return new CacheDecision(false, false, reason);
    }
}
