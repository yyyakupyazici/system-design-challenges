package io.paramix.llmgw.provider;

import java.util.List;

import io.paramix.llmgw.api.ChatCompletionRequest;

/**
 * Sağlayıcıya giden istek (PII maskelenmiş mesajlar).
 *
 * @param promptCache sağlayıcı tarafı prompt caching istensin mi (sohbetlerde kâr, tek atımlık işlerde zarar)
 */
public record ProviderRequest(String providerModel, List<ChatCompletionRequest.Message> messages,
                              int maxTokens, Double temperature, boolean promptCache) {
}
