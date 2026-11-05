package io.paramix.llmgw.provider;

import java.util.List;

import io.paramix.llmgw.api.ChatCompletionRequest;
import io.paramix.llmgw.config.GatewayProperties.ProviderType;
import io.paramix.llmgw.pricing.TokenUsage;

import org.springframework.ai.anthropic.AnthropicChatOptions;
import org.springframework.ai.anthropic.api.AnthropicApi;
import org.springframework.ai.anthropic.api.AnthropicCacheOptions;
import org.springframework.ai.anthropic.api.AnthropicCacheStrategy;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.ollama.api.OllamaChatOptions;
import org.springframework.ai.openai.OpenAiChatOptions;
import reactor.core.publisher.Flux;

/**
 * Spring AI {@link ChatModel} üzerine adaptör. Sağlayıcıya özgü iki şeyi burada çözer:
 * <ul>
 *   <li>Seçenekler: Anthropic'te prompt caching açıkça istenir ({@code CONVERSATION_HISTORY}); OpenAI'de
 *       stream'in sonunda usage gelmesi için {@code streamUsage} açılır.</li>
 *   <li>Kullanım: Anthropic cache okuma/yazma token'larını ayrı bildirir; dört fiyat kalemine ayrıştırılır.</li>
 * </ul>
 */
public class SpringAiProviderClient implements ProviderClient {

    private final String name;
    private final ProviderType type;
    private final boolean internal;
    private final ChatModel model;

    public SpringAiProviderClient(String name, ProviderType type, boolean internal, ChatModel model) {
        this.name = name;
        this.type = type;
        this.internal = internal;
        this.model = model;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean internal() {
        return internal;
    }

    public ChatModel chatModel() {
        return model;
    }

    @Override
    public ProviderReply call(ProviderRequest r) {
        return toReply(model.call(prompt(r)));
    }

    @Override
    public Flux<ProviderReply> stream(ProviderRequest r) {
        return model.stream(prompt(r)).map(SpringAiProviderClient::toReply);
    }

    Prompt prompt(ProviderRequest r) {
        List<Message> messages = r.messages().stream().map(SpringAiProviderClient::toMessage).toList();
        return new Prompt(messages, options(r));
    }

    private ChatOptions options(ProviderRequest r) {
        return switch (type) {
            case FAKE -> new FakeChatOptions(r.providerModel(), r.maxTokens(), r.temperature(), r.promptCache());
            case ANTHROPIC -> AnthropicChatOptions.builder()
                    .model(r.providerModel())
                    .maxTokens(r.maxTokens())
                    .temperature(r.temperature())
                    .cacheOptions(r.promptCache()
                            ? AnthropicCacheOptions.builder().strategy(AnthropicCacheStrategy.CONVERSATION_HISTORY).build()
                            : AnthropicCacheOptions.DISABLED)
                    .build();
            case OPENAI -> OpenAiChatOptions.builder()
                    .model(r.providerModel())
                    .maxTokens(r.maxTokens())
                    .temperature(r.temperature())
                    .streamUsage(true)
                    .build();
            case OLLAMA -> OllamaChatOptions.builder()
                    .model(r.providerModel())
                    .numPredict(r.maxTokens())
                    .temperature(r.temperature())
                    .build();
        };
    }

    private static Message toMessage(ChatCompletionRequest.Message m) {
        return switch (m.role()) {
            case "system" -> new SystemMessage(m.content());
            case "assistant" -> new AssistantMessage(m.content());
            default -> new UserMessage(m.content());
        };
    }

    static ProviderReply toReply(ChatResponse resp) {
        String text = resp.getResult() == null || resp.getResult().getOutput() == null
                ? "" : nullToEmpty(resp.getResult().getOutput().getText());
        String finish = resp.getResult() == null || resp.getResult().getMetadata() == null
                ? null : resp.getResult().getMetadata().getFinishReason();
        return new ProviderReply(text, usage(resp), finish == null || finish.isBlank() ? null : finish);
    }

    /** Usage yoksa (stream'in ara parçaları) null. */
    static TokenUsage usage(ChatResponse resp) {
        if (resp.getMetadata() == null) {
            return null;
        }
        Usage u = resp.getMetadata().getUsage();
        if (u == null) {
            return null;
        }
        Object nativeUsage = u.getNativeUsage();
        if (nativeUsage instanceof TokenUsage t) {
            return t;
        }
        if (nativeUsage instanceof AnthropicApi.Usage a) {
            return new TokenUsage(zero(a.inputTokens()), zero(a.cacheReadInputTokens()),
                    zero(a.cacheCreationInputTokens()), zero(a.outputTokens()));
        }
        int prompt = zero(u.getPromptTokens());
        int completion = zero(u.getCompletionTokens());
        if (prompt == 0 && completion == 0) {
            return null;
        }
        // OpenAI/vLLM/Ollama: cache ayrımı yok → hepsi normal fiyattan (temkinli)
        return TokenUsage.of(prompt, completion);
    }

    private static int zero(Integer i) {
        return i == null ? 0 : i;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
