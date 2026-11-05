package io.paramix.llmgw.pricing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.paramix.llmgw.config.GatewayProperties.ModelProperties;
import io.paramix.llmgw.config.GatewayProperties.Tier;

import org.junit.jupiter.api.Test;

/** IDE'deki fiyat listesi: input 200, output 1000, cache read 10, cache write 250 (1M token başına kredi). */
class CostCalculatorTest {

    static final ModelProperties SONNET_LIKE = new ModelProperties("p", "m", Tier.LARGE, "v1",
            200, 1000, 10, 250, 1024, 4096, 200000, List.of());

    @Test
    void reservationAssumesNoCacheDiscountAndFullMaxTokens() {
        // 10.000 token girdi (cache yazma fiyatı daha pahalı → 250) + 2.000 çıktı
        long max = CostCalculator.maxCostMicro(SONNET_LIKE, 10_000, 2_000);
        assertThat(CostCalculator.toCredits(max)).isEqualTo(2.5 + 2.0);
    }

    /**
     * Sohbetin 3 turu; her tur sistem promptu + geçmiş (önek) + yeni soru. Önek: 1. tur 4.000, 2. tur 5.000,
     * 3. tur 6.000 token; her turun yeni sorusu 500, cevabı 800 token.
     */
    @Test
    void threeTurnConversationWithAndWithoutPromptCaching() {
        long without = CostCalculator.actualCostMicro(SONNET_LIKE, TokenUsage.of(4_500, 800))
                + CostCalculator.actualCostMicro(SONNET_LIKE, TokenUsage.of(5_500, 800))
                + CostCalculator.actualCostMicro(SONNET_LIKE, TokenUsage.of(6_500, 800));
        // Cache'li: 1. tur öneki yazar; sonraki turlar önceki öneki okur, sadece eklenen kısmı yazar.
        long with = CostCalculator.actualCostMicro(SONNET_LIKE, new TokenUsage(500, 0, 4_000, 800))
                + CostCalculator.actualCostMicro(SONNET_LIKE, new TokenUsage(500, 4_000, 1_000, 800))
                + CostCalculator.actualCostMicro(SONNET_LIKE, new TokenUsage(500, 5_000, 1_000, 800));

        assertThat(CostCalculator.toCredits(without)).isEqualTo(5.7);
        assertThat(CostCalculator.toCredits(with)).isEqualTo(4.29);
        assertThat(with).isLessThan(without);
    }
}
