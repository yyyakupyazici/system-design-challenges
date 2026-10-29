package io.paramix.notify.quota;

import java.util.function.LongSupplier;

/**
 * Pod içi token bucket. Redis'e ulaşılamadığında (degraded mode) kullanılır:
 * her pod, global payın kabaca 1/N'ini harcar → toplamda limit aşılmaz, OTP akmaya devam eder.
 */
public class LocalTokenBucket {

    private final double ratePerSec;
    private final double capacity;
    private final LongSupplier nanoClock;
    private double tokens;
    private long last;

    public LocalTokenBucket(double ratePerSec, LongSupplier nanoClock) {
        this.ratePerSec = ratePerSec;
        this.capacity = Math.max(1, ratePerSec);
        this.nanoClock = nanoClock;
        this.tokens = capacity;
        this.last = nanoClock.getAsLong();
    }

    /** 0 → izin verildi; aksi halde kaç ms sonra tekrar denenmeli. */
    public synchronized long tryAcquire() {
        long now = nanoClock.getAsLong();
        tokens = Math.min(capacity, tokens + (now - last) / 1_000_000_000.0 * ratePerSec);
        last = now;
        if (tokens >= 1) {
            tokens -= 1;
            return 0;
        }
        if (ratePerSec <= 0) {
            return 1000;
        }
        return Math.max(1, (long) Math.ceil((1 - tokens) * 1000 / ratePerSec));
    }
}
