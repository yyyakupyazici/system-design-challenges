package io.paramix.notify.quota;

public record QuotaDecision(boolean allowed, long waitMs, Source source) {

    public enum Source { RESERVED, SHARED, LOCAL_FALLBACK, DENIED }

    static QuotaDecision denied(long waitMs) {
        return new QuotaDecision(false, Math.max(1, waitMs), Source.DENIED);
    }
}
