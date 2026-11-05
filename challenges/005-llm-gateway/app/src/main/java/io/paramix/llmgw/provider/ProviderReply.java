package io.paramix.llmgw.provider;

import io.paramix.llmgw.pricing.TokenUsage;

/** Sağlayıcı cevabı (stream'de her parça da bu tipte; usage genelde sadece son parçada dolu). */
public record ProviderReply(String text, TokenUsage usage, String finishReason) {
}
