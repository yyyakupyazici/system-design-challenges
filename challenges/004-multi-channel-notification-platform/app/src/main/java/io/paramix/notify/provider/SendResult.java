package io.paramix.notify.provider;

/**
 * Sağlayıcı çağrısının sonucu. En önemli ayrım: {@link Unavailable} "kesin gitmedi",
 * {@link Timeout} "gitti mi bilmiyoruz" demektir. İkincisi asla körlemesine tekrar denenmez.
 */
public sealed interface SendResult {

    record Accepted(String providerMsgId) implements SendResult {
    }

    /** 429: sağlayıcının limiti doldu. AIMD hızı düşürür, başka sağlayıcı denenebilir. */
    record RateLimited(String detail) implements SendResult {
    }

    /** Geçersiz numara, ölü push token'ı vb. Tekrar denemek anlamsız. */
    record PermanentFailure(String code) implements SendResult {
    }

    /** Bağlantı kurulamadı / 503: istek sağlayıcıya ulaşmadı, güvenle başka sağlayıcı denenebilir. */
    record Unavailable(String detail) implements SendResult {
    }

    /** İstek gitti, cevap gelmedi. Mesaj gönderilmiş de olabilir: önce sağlayıcıya sorulur (reconcile). */
    record Timeout(String detail) implements SendResult {
    }
}
