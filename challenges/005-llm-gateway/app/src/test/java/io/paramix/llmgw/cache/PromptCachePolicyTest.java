package io.paramix.llmgw.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.paramix.llmgw.TestProps;
import io.paramix.llmgw.api.ChatCompletionRequest.Message;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.config.GatewayProperties.AppType;

import org.junit.jupiter.api.Test;

class PromptCachePolicyTest {

    static final String LONG_SYSTEM = "Sen bir bankacılık asistanısın. ".repeat(200);   // ~1.600 token

    @Test
    void chatWithSessionAndLongPrefixUsesPromptCache() {
        List<Message> msgs = List.of(new Message("system", LONG_SYSTEM), new Message("user", "Merhaba"));
        assertThat(PromptCachePolicy.usePromptCache(TestProps.chat(false, "s-1"), msgs, 1024)).isTrue();
    }

    @Test
    void priorAssistantTurnIsEnoughEvenWithoutSessionId() {
        List<Message> msgs = List.of(new Message("system", LONG_SYSTEM), new Message("user", "a"),
                new Message("assistant", "b"), new Message("user", "c"));
        assertThat(PromptCachePolicy.usePromptCache(TestProps.chat(false, null), msgs, 1024)).isTrue();
    }

    @Test
    void oneShotRequestsDoNotPayTheWritePremium() {
        List<Message> msgs = List.of(new Message("system", LONG_SYSTEM), new Message("user", "Bu dokümanı özetle"));
        ClientContext batch = new ClientContext("t", "a", AppType.BATCH, true, "u", false, "s-1");
        assertThat(PromptCachePolicy.usePromptCache(batch, msgs, 1024)).isFalse();
        assertThat(PromptCachePolicy.usePromptCache(TestProps.chat(false, null), msgs, 1024)).isFalse();
    }

    @Test
    void shortPrefixIsNotWorthCaching() {
        List<Message> msgs = List.of(new Message("system", "Kısa"), new Message("user", "Merhaba"));
        assertThat(PromptCachePolicy.usePromptCache(TestProps.chat(false, "s-1"), msgs, 1024)).isFalse();
    }
}
