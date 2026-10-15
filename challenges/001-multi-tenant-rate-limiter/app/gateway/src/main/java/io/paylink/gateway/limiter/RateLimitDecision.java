package io.paylink.gateway.limiter;

import java.util.List;

/**
 * Lua script'inin kararı.
 *
 * @param quotaRemaining -1 = kota tanımlı değil
 * @param quotaKey       null = kota yok (refund için gerekli)
 */
public record RateLimitDecision(
        boolean allowed,
        Reason reason,
        long remaining,
        long limit,
        long retryAfterMs,
        long resetMs,
        long quotaRemaining,
        String quotaKey) {

    public enum Reason {
        OK, RATE, QUOTA, GLOBAL, FAIL_OPEN, FAIL_CLOSED
    }

    static RateLimitDecision fromScript(List<?> result, String quotaKey) {
        long allowed = num(result, 0);
        int reason = (int) num(result, 1);
        Reason r = switch (reason) {
            case 0 -> Reason.OK;
            case 1 -> Reason.RATE;
            case 2 -> Reason.QUOTA;
            default -> throw new IllegalStateException("Bilinmeyen reason: " + reason);
        };
        return new RateLimitDecision(allowed == 1, r, num(result, 2), num(result, 3),
                num(result, 4), num(result, 5), num(result, 6), quotaKey);
    }

    static RateLimitDecision failOpen() {
        return new RateLimitDecision(true, Reason.FAIL_OPEN, -1, -1, 0, 0, -1, null);
    }

    static RateLimitDecision failClosed() {
        return new RateLimitDecision(false, Reason.FAIL_CLOSED, 0, -1, 1000, 1000, -1, null);
    }

    RateLimitDecision asGlobalRejection() {
        return new RateLimitDecision(false, Reason.GLOBAL, remaining, limit, retryAfterMs, resetMs, quotaRemaining, quotaKey);
    }

    private static long num(List<?> result, int index) {
        Object value = result.get(index);
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }
}
