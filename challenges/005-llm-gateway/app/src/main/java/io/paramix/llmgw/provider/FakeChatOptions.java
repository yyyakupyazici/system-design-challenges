package io.paramix.llmgw.provider;

import java.util.List;

import org.springframework.ai.chat.prompt.ChatOptions;

/** {@link FakeChatModel}'e "prompt caching istendi mi" bilgisini taşıyan seçenekler. */
public record FakeChatOptions(String model, Integer maxTokens, Double temperature, boolean promptCache) implements ChatOptions {

    @Override
    public String getModel() {
        return model;
    }

    @Override
    public Double getFrequencyPenalty() {
        return null;
    }

    @Override
    public Integer getMaxTokens() {
        return maxTokens;
    }

    @Override
    public Double getPresencePenalty() {
        return null;
    }

    @Override
    public List<String> getStopSequences() {
        return null;
    }

    @Override
    public Double getTemperature() {
        return temperature;
    }

    @Override
    public Integer getTopK() {
        return null;
    }

    @Override
    public Double getTopP() {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ChatOptions> T copy() {
        return (T) new FakeChatOptions(model, maxTokens, temperature, promptCache);
    }
}
