package io.paramix.notify.worker;

import java.time.Duration;

import io.paramix.notify.domain.Priority;

/**
 * Öncelik bazlı teslimat kararları. Saf fonksiyonlar, birim testli.
 *
 * <pre>
 *                 kota bekleme   yedek sağlayıcı   UNKNOWN + sorgulanamıyor
 *  CRITICAL       300 ms         evet              tekrar gönder  (iki OTP zararsız, hiç gelmemesi değil)
 *  TRANSACTIONAL  2 sn           evet              tekrar gönder  (kaybolması iki kez gelmesinden kötü)
 *  MARKETING      30 sn          hayır             gönderme       (iki kampanya SMS'i = para + şikayet)
 * </pre>
 */
public final class DeliveryPolicy {

    public enum UnknownAction { RESEND, ABANDON }

    private DeliveryPolicy() {
    }

    public static Duration maxQuotaWait(Priority p) {
        return switch (p) {
            case CRITICAL -> Duration.ofMillis(300);
            case TRANSACTIONAL -> Duration.ofSeconds(2);
            case MARKETING -> Duration.ofSeconds(30);
        };
    }

    /** Pahalı yedek sağlayıcıyı kampanyaya harcamayız. */
    public static boolean mayUseFallbackProvider(Priority p) {
        return p != Priority.MARKETING;
    }

    public static UnknownAction onUnknown(Priority p) {
        return p == Priority.MARKETING ? UnknownAction.ABANDON : UnknownAction.RESEND;
    }

    /** Hiçbir sağlayıcıdan izin/cevap alınamadığında mesajın kaç ms sonra tekrar deneneceği. */
    public static long retryDelayMs(Priority p) {
        return switch (p) {
            case CRITICAL -> 200;
            case TRANSACTIONAL -> 500;
            case MARKETING -> 1000;
        };
    }
}
