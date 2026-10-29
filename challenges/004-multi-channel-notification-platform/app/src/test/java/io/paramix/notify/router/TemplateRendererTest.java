package io.paramix.notify.router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

class TemplateRendererTest {

    @Test
    void replacesPlaceholders() {
        String out = TemplateRenderer.render("{{sender}} sana {{ amount }} {{currency}} gönderdi.",
                Map.of("sender", "Ayşe K.", "amount", "500,00", "currency", "TRY"));
        assertThat(out).isEqualTo("Ayşe K. sana 500,00 TRY gönderdi.");
    }

    @Test
    void valuesWithRegexCharactersAreInsertedLiterally() {
        assertThat(TemplateRenderer.render("Tutar: {{a}}", Map.of("a", "$1.000 \\o/"))).isEqualTo("Tutar: $1.000 \\o/");
    }

    @Test
    void missingParameter_isAnError_notAHalfMessage() {
        assertThatThrownBy(() -> TemplateRenderer.render("Kodun: {{code}}", Map.of()))
                .isInstanceOf(TemplateRenderer.MissingParameterException.class)
                .hasMessageContaining("code");
    }

    @Test
    void nullTemplate_staysNull() {
        assertThat(TemplateRenderer.render(null, Map.of())).isNull();
    }
}
