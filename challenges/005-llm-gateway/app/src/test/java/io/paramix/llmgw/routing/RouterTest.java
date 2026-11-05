package io.paramix.llmgw.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.paramix.llmgw.TestProps;
import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.core.GatewayException;

import org.junit.jupiter.api.Test;

class RouterTest {

    final GatewayProperties props = TestProps.props();

    @Test
    void autoPicksSmallForSimpleQuestions() {
        Router.Plan p = Router.route(props, "auto", false, 50, "EFT ile havale farkı nedir?");
        assertThat(p.primary()).isEqualTo("small");
        assertThat(p.candidates()).containsExactly("small", "small-backup");
    }

    @Test
    void autoPicksLargeForLongInputOrHardKeywords() {
        assertThat(Router.route(props, "auto", false, 5000, "özetle").primary()).isEqualTo("large");
        assertThat(Router.route(props, "auto", false, 50, "Bu iki kampanyayı KARŞILAŞTIR").primary()).isEqualTo("large");
    }

    @Test
    void confidentialDataNeverLeavesInternalProviders() {
        Router.Plan auto = Router.route(props, "auto", true, 50, "müşteri dosyası");
        assertThat(auto.candidates()).containsExactly("internal");

        assertThatThrownBy(() -> Router.route(props, "large", true, 50, "x"))
                .isInstanceOf(GatewayException.class)
                .extracting(e -> ((GatewayException) e).code()).isEqualTo("data_class_violation");
    }

    @Test
    void fallbackChainKeepsConfiguredOrder() {
        assertThat(Router.route(props, "large", false, 50, "x").candidates())
                .containsExactly("large", "large-backup", "internal");
    }

    @Test
    void unknownModelIs404() {
        assertThatThrownBy(() -> Router.route(props, "gpt-x", false, 50, "x"))
                .extracting(e -> ((GatewayException) e).code()).isEqualTo("model_not_found");
    }

    @Test
    void cheapestIgnoresLargeTier() {
        assertThat(Router.cheapest(props, false)).contains("internal");
        assertThat(Router.cheapest(props, true)).contains("internal");
    }
}
