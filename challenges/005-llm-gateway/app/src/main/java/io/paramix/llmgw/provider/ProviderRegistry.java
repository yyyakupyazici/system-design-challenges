package io.paramix.llmgw.provider;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.ProviderProperties;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.ai.anthropic.AnthropicChatModel;
import org.springframework.ai.anthropic.AnthropicChatOptions;
import org.springframework.ai.anthropic.api.AnthropicApi;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.ollama.api.OllamaApi;
import org.springframework.ai.ollama.api.OllamaChatOptions;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.stereotype.Component;

/**
 * Config'teki sağlayıcılardan adaptörleri kurar. API anahtarları env/Secret'tan gelir, koda hiç girmez.
 * Kendi GPU kümemizdeki vLLM, OpenAI-uyumlu API sunduğu için OPENAI tipiyle (baseUrl = küme içi servis) tanımlanır.
 * Sağlayıcı başına bir circuit breaker: hata ya da yavaşlık oranı eşiği geçince o sağlayıcı bir süre atlanır.
 */
@Component
public class ProviderRegistry {

    private final Map<String, ProviderClient> clients = new LinkedHashMap<>();
    private final CircuitBreakerRegistry breakers;

    public ProviderRegistry(GatewayProperties props) {
        props.providers().forEach((name, p) -> clients.put(name, build(name, p)));
        this.breakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofSeconds(20))
                .slowCallRateThreshold(80)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build());
    }

    public ProviderClient get(String name) {
        ProviderClient c = clients.get(name);
        if (c == null) {
            throw new IllegalArgumentException("Bilinmeyen sağlayıcı: " + name);
        }
        return c;
    }

    public CircuitBreaker breaker(String name) {
        return breakers.circuitBreaker(name);
    }

    /** Testler ve lokal demo: FAKE tipli sağlayıcının modeline erişim. */
    public FakeChatModel fake(String name) {
        return (FakeChatModel) ((SpringAiProviderClient) get(name)).chatModel();
    }

    private static ProviderClient build(String name, ProviderProperties p) {
        ChatModel model = switch (p.type()) {
            case FAKE -> new FakeChatModel(name);
            case ANTHROPIC -> AnthropicChatModel.builder()
                    .anthropicApi(baseUrl(AnthropicApi.builder().apiKey(require(name, p.apiKey())), p.baseUrl()))
                    .defaultOptions(AnthropicChatOptions.builder().maxTokens(1024).build())
                    .build();
            case OPENAI -> OpenAiChatModel.builder()
                    .openAiApi(openAi(p))
                    .defaultOptions(OpenAiChatOptions.builder().build())
                    .build();
            case OLLAMA -> OllamaChatModel.builder()
                    .ollamaApi(OllamaApi.builder().baseUrl(p.baseUrl() == null ? "http://localhost:11434" : p.baseUrl()).build())
                    .defaultOptions(OllamaChatOptions.builder().build())
                    .build();
        };
        return new SpringAiProviderClient(name, p.type(), p.internal(), model);
    }

    private static AnthropicApi baseUrl(AnthropicApi.Builder b, String url) {
        return (url == null || url.isBlank() ? b : b.baseUrl(url)).build();
    }

    private static OpenAiApi openAi(ProviderProperties p) {
        OpenAiApi.Builder b = OpenAiApi.builder().apiKey(p.apiKey() == null || p.apiKey().isBlank() ? "not-needed" : p.apiKey());
        if (p.baseUrl() != null && !p.baseUrl().isBlank()) {
            b.baseUrl(p.baseUrl());
        }
        return b.build();
    }

    private static String require(String name, String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("Sağlayıcı " + name + " için API anahtarı yok (env/Secret ile verin)");
        }
        return key;
    }
}
