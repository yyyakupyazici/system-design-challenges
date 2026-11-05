package io.paramix.llmgw.usage;

import java.time.Instant;

/** Her isteğin kalıcı kaydı. Prompt ve cevap MASKELENMİŞ haliyle saklanır (30 gün). */
public record UsageEvent(
        String requestId, Instant at, String team, String app, String userId,
        String requestedModel, String routedModel, String provider, String fallbackFrom,
        String cacheStatus, boolean promptCache,
        int inputTokens, int cacheReadTokens, int cacheWriteTokens, int outputTokens,
        long costMicro, long latencyMs, Long ttftMs, String status, int piiMasked,
        String maskedPrompt, String maskedResponse) {
}
