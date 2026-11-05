package io.paramix.llmgw.pricing;

import io.paramix.llmgw.config.GatewayProperties.ModelProperties;

/**
 * Fiyatlar "1M token başına kredi". Maliyet mikro-kredi (1 kredi = 1.000.000 mikro-kredi) ile tutulur:
 * böylece {@code token × fiyat} doğrudan mikro-krediye eşittir, float yok, kuruş kaybı yok.
 */
public final class CostCalculator {

    private CostCalculator() {
    }

    /**
     * Rezervasyon: isteğin EN FAZLA tutarı. Cache indirimi varsayılmaz (cache'in süresi dolmuş olabilir),
     * çıktı max_tokens kadar sayılır. Gerçek tutar mutabakatta düzeltilir.
     */
    public static long maxCostMicro(ModelProperties m, int inputTokens, int maxOutputTokens) {
        long inputWorst = Math.max(m.inputPrice(), m.cacheWritePrice());
        return inputTokens * inputWorst + (long) maxOutputTokens * m.outputPrice();
    }

    public static long actualCostMicro(ModelProperties m, TokenUsage u) {
        return (long) u.uncachedInput() * m.inputPrice()
                + (long) u.cacheRead() * m.cacheReadPrice()
                + (long) u.cacheWrite() * m.cacheWritePrice()
                + (long) u.output() * m.outputPrice();
    }

    public static double toCredits(long micro) {
        return micro / 1_000_000.0;
    }
}
