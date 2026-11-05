package io.paramix.llmgw.pricing;

/**
 * Kaba token tahmini: ~4 karakter = 1 token. Rezervasyon ve hız limiti için yeterli; kesin sayı
 * sağlayıcının usage bilgisinden mutabakatta gelir. Prod'da modelin kendi tokenizer'ı kullanılabilir.
 */
public final class TokenEstimator {

    private static final int PER_MESSAGE_OVERHEAD = 4;

    private TokenEstimator() {
    }

    public static int estimate(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        return (text.length() + 3) / 4;
    }

    public static int estimateMessages(Iterable<String> contents) {
        int total = 0;
        for (String c : contents) {
            total += estimate(c) + PER_MESSAGE_OVERHEAD;
        }
        return total;
    }
}
