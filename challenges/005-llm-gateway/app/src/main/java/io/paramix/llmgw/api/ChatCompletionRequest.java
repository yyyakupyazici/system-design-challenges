package io.paramix.llmgw.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;

/** OpenAI {@code /v1/chat/completions} isteğinin gateway'in kullandığı alt kümesi. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChatCompletionRequest(
        @NotBlank String model,
        @NotEmpty @Valid List<Message> messages,
        @JsonProperty("max_tokens") @Positive Integer maxTokens,
        Double temperature,
        boolean stream) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Message(@NotBlank String role, @NotBlank String content) {
    }
}
