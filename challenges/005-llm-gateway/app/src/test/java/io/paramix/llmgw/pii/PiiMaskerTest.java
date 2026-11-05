package io.paramix.llmgw.pii;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PiiMaskerTest {

    // Geçerli test değerleri: TCKN 10000000146 (checksum tutar), IBAN TR33 0006 1005 1978 6457 8413 26 (mod-97), kart 4111 1111 1111 1111 (Luhn)
    @Test
    void masksValidIdentifiersAndRestoresThem() {
        PiiVault vault = new PiiVault();
        String text = "TC 10000000146, IBAN TR33 0006 1005 1978 6457 8413 26, kart 4111 1111 1111 1111, "
                + "tel 0532 123 45 67, mail ali@example.com";
        String masked = PiiMasker.mask(text, vault);

        assertThat(masked).doesNotContain("10000000146", "TR33", "4111", "532", "ali@example.com")
                .contains("<TCKN_1>", "<IBAN_1>", "<CARD_1>", "<PHONE_1>", "<EMAIL_1>");
        assertThat(vault.size()).isEqualTo(5);
        assertThat(vault.unmask(masked)).isEqualTo(text);
    }

    @Test
    void invalidChecksumsAreNotMasked() {
        PiiVault vault = new PiiVault();
        String text = "sipariş no 12345678901, kart 4111 1111 1111 1112";
        assertThat(PiiMasker.mask(text, vault)).isEqualTo(text);
        assertThat(vault.isEmpty()).isTrue();
    }

    @Test
    void sameValueGetsSamePlaceholder() {
        PiiVault vault = new PiiVault();
        String masked = PiiMasker.mask("a@b.com ve tekrar a@b.com, ayrıca c@d.com", vault);
        assertThat(masked).isEqualTo("<EMAIL_1> ve tekrar <EMAIL_1>, ayrıca <EMAIL_2>");
    }

    @Test
    void streamingUnmaskerHandlesPlaceholderSplitAcrossChunks() {
        PiiVault vault = new PiiVault();
        PiiMasker.mask("mail ali@example.com", vault);
        StreamingUnmasker un = new StreamingUnmasker(vault);

        StringBuilder out = new StringBuilder();
        for (String chunk : new String[] {"Adresiniz <EM", "AIL", "_1> olarak kay", "ıtlı. 3 < 5"}) {
            out.append(un.accept(chunk));
        }
        out.append(un.flush());
        assertThat(out.toString()).isEqualTo("Adresiniz ali@example.com olarak kayıtlı. 3 < 5");
    }
}
