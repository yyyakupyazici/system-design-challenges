package io.paramix.llmgw.cache;

/** Cache'te saklanan cevap (maskelenmiş haliyle; PII'li istekler zaten cache'lenmez). */
public record CachedAnswer(String content, String model, int promptTokens, int completionTokens) {
}
