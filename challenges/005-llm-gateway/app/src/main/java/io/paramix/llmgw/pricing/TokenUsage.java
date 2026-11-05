package io.paramix.llmgw.pricing;

/**
 * Sağlayıcının bildirdiği kullanım, dört fiyat kalemine ayrılmış hali.
 *
 * @param uncachedInput cache'e girmeyen, normal fiyattan sayılan girdi
 * @param cacheRead cache'ten okunan girdi (ucuz)
 * @param cacheWrite bu istekte cache'e yazılan girdi (normalden pahalı)
 */
public record TokenUsage(int uncachedInput, int cacheRead, int cacheWrite, int output) {

    public int totalInput() {
        return uncachedInput + cacheRead + cacheWrite;
    }

    public static TokenUsage of(int input, int output) {
        return new TokenUsage(input, 0, 0, output);
    }
}
