package io.paramix.notify.worker;

/** Kafka error handler bu istisnayı görünce kaydı sınırsız kez, delayMs aralıkla yeniden dener (backpressure). */
public class RetryLaterException extends RuntimeException {

    private final long delayMs;

    public RetryLaterException(String message, long delayMs) {
        super(message, null, false, false);
        this.delayMs = delayMs;
    }

    public long delayMs() {
        return delayMs;
    }
}
