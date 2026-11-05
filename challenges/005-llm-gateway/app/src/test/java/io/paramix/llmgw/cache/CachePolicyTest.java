package io.paramix.llmgw.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.paramix.llmgw.TestProps;
import io.paramix.llmgw.api.ChatCompletionRequest;
import io.paramix.llmgw.api.ChatCompletionRequest.Message;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.config.GatewayProperties.AppType;
import io.paramix.llmgw.pii.PiiMasker;
import io.paramix.llmgw.pii.PiiVault;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class CachePolicyTest {

    static CacheDecision decide(ClientContext ctx, String question, Double temperature) {
        PiiVault vault = new PiiVault();
        String masked = PiiMasker.mask(question, vault);
        ChatCompletionRequest req = new ChatCompletionRequest("auto", List.of(new Message("user", question)), null,
                temperature, false);
        return CachePolicy.decide(ctx, req, masked, vault.size(), TestProps.cache());
    }

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource(delimiter = '|', value = {
            "Kartımı nasıl iptal ederim?                    | true  | ",
            "Hesabımda ne kadar para var?                   | false | personal",
            "TR33 0006 1005 1978 6457 8413 26 IBAN'a 500 TL gönder | false | contains_pii",
            "EFT ile havale farkı nedir?                    | true  | ",
            "Bu ayki harcamalarımı özetle                   | false | personal",
            "Annemin hesabına 200 TL havale yap             | false | action",
    })
    void casesFromTheDesign(String question, boolean cacheable, String reason) {
        CacheDecision d = decide(TestProps.chat(false, null), question.trim(), null);
        assertThat(d.exact()).isEqualTo(cacheable);
        assertThat(d.reason()).isEqualTo(reason);
    }

    @Test
    void confidentialAppOptOutAndHighTemperatureAreNeverCached() {
        assertThat(decide(TestProps.chat(true, null), "EFT nedir?", null).reason()).isEqualTo("confidential");
        assertThat(decide(new ClientContext("t", "a", AppType.CHAT, false, "u", false, null), "EFT nedir?", null).reason())
                .isEqualTo("app_opt_out");
        assertThat(decide(TestProps.chat(false, null), "Bir slogan yaz", 0.9).reason()).isEqualTo("high_temperature");
    }

    @Test
    void semanticOnlyForSingleTurn() {
        ChatCompletionRequest multi = new ChatCompletionRequest("auto", List.of(
                new Message("user", "EFT nedir?"), new Message("assistant", "..."), new Message("user", "peki ya ikincisi?")),
                null, null, false);
        CacheDecision d = CachePolicy.decide(TestProps.chat(false, null), multi, "peki ya ikincisi?", 0, TestProps.cache());
        assertThat(d.exact()).isTrue();
        assertThat(d.semantic()).isFalse();
    }
}
