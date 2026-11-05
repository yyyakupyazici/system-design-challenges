package io.paramix.llmgw.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** OpenAI-uyumlu cevap + gateway'in eklediği {@code x_gateway} bilgisi (cache, sağlayıcı, maliyet). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ChatCompletionResponse(String id, String object, long created, String model,
                                     List<Choice> choices, Usage usage,
                                     @JsonProperty("x_gateway") GatewayInfo gateway) {

    public record Choice(int index, ChatCompletionRequest.Message message,
                         @JsonProperty("finish_reason") String finishReason) {
    }

    public record Usage(@JsonProperty("prompt_tokens") int promptTokens,
                        @JsonProperty("completion_tokens") int completionTokens,
                        @JsonProperty("total_tokens") int totalTokens,
                        @JsonProperty("prompt_tokens_details") PromptDetails promptTokensDetails) {
    }

    public record PromptDetails(@JsonProperty("cached_tokens") int cachedTokens,
                                @JsonProperty("cache_write_tokens") int cacheWriteTokens) {
    }

    /**
     * @param cache MISS, HIT_EXACT, HIT_SEMANTIC
     * @param fallbackFrom birincil model hata verdiyse onun adı
     */
    public record GatewayInfo(String cache, String routedModel, String provider, String fallbackFrom,
                              double costCredits, boolean promptCache, int piiMasked) {
    }
}
