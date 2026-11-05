package io.paramix.llmgw.cache;

import java.util.List;

import io.paramix.llmgw.api.ChatCompletionRequest;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.config.GatewayProperties.AppType;
import io.paramix.llmgw.pricing.TokenEstimator;

/**
 * Sağlayıcı tarafı prompt caching açılsın mı? Cache yazma normal girdiden pahalı (ör. 250 vs 200), okuma çok ucuz
 * (ör. 10). Yani kâr ancak aynı önek bir daha okunursa gelir:
 *
 * <ul>
 *   <li>BATCH (tek atımlık toplu iş: her doküman bir kez) → hayır: yazma primi boşa gider.</li>
 *   <li>CHAT + oturum kimliği ya da önceki turlar var → evet: sonraki tur aynı öneki okuyacak.</li>
 *   <li>Önek çok kısa → hayır: sağlayıcılar genelde belli bir uzunluğun altını cache'lemez, prim kazancı yemez.</li>
 * </ul>
 */
public final class PromptCachePolicy {

    private PromptCachePolicy() {
    }

    public static boolean usePromptCache(ClientContext ctx, List<ChatCompletionRequest.Message> masked, int minPrefixTokens) {
        if (ctx.appType() != AppType.CHAT || masked.size() < 2) {
            return false;
        }
        boolean conversational = ctx.sessionId() != null || masked.stream().anyMatch(m -> "assistant".equals(m.role()));
        if (!conversational) {
            return false;
        }
        int prefix = TokenEstimator.estimateMessages(masked.subList(0, masked.size() - 1).stream()
                .map(ChatCompletionRequest.Message::content).toList());
        return prefix >= minPrefixTokens;
    }
}
