package io.paramix.chat.gateway;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Yeni bağlantı kabul hızı (yerel token bucket). Geri bağlanma fırtınasında 50.000 istemci aynı saniyede gelirse
 * bu gateway saniyede en fazla N tanesini kabul eder; diğerleri 503 + rastgele bir "Retry-After" alır ve
 * kendiliğinden zamana yayılır. Fırtınayı durdurmaz, düzleştirir.
 */
public class AdmissionLimiter {

    private final double ratePerNano;
    private final double capacity;
    private double tokens;
    private long last;

    public AdmissionLimiter(int perSecond) {
        this.capacity = perSecond;
        this.ratePerNano = perSecond / 1_000_000_000.0;
        this.tokens = perSecond;
        this.last = System.nanoTime();
    }

    public synchronized boolean tryAcquire() {
        long now = System.nanoTime();
        tokens = Math.min(capacity, tokens + (now - last) * ratePerNano);
        last = now;
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }

    /** Reddedilen istemciye önerilen bekleme: 1–10 sn arası rastgele (hepsi aynı anda tekrar gelmesin). */
    public static int retryAfterSeconds() {
        return 1 + ThreadLocalRandom.current().nextInt(10);
    }
}
