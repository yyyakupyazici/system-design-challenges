package io.paramix.notify.config;

import io.paramix.notify.worker.RetryLaterException;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Router ve worker için ortak hata politikası. Kural: mesaj ASLA sessizce kaybolmaz.
 * <ul>
 *   <li>{@link RetryLaterException} (kota yok / sağlayıcı yok) → kendi aralığıyla sınırsız tekrar (backpressure).</li>
 *   <li>Geçici hata (DB, Kafka, Redis) → 0,5 sn'den 10 sn'ye artan aralıkla sınırsız tekrar.
 *       Offset commit edilmez; altyapı düzelince kaldığı yerden devam eder.</li>
 *   <li>Bozuk mesaj (JSON, geçersiz alan) → tekrar denemek anlamsız: loglanıp atlanır (prod'da DLQ topic'i).</li>
 * </ul>
 */
public final class KafkaErrorHandlers {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlers.class);

    private KafkaErrorHandlers() {
    }

    public static DefaultErrorHandler create() {
        ExponentialBackOff transientBackOff = new ExponentialBackOff(500, 2.0);
        transientBackOff.setMaxInterval(10_000);   // maxElapsedTime varsayılanı sınırsız

        DefaultErrorHandler h = new DefaultErrorHandler(
                (rec, ex) -> log.error("Bozuk mesaj atlanıyor ({}): {}", rec.topic(), rec.value(), ex),
                transientBackOff);
        h.addNotRetryableExceptions(IllegalArgumentException.class, JsonProcessingException.class);
        h.setBackOffFunction((rec, ex) -> {
            RetryLaterException rl = findRetryLater(ex);
            return rl == null ? null : new FixedBackOff(rl.delayMs(), FixedBackOff.UNLIMITED_ATTEMPTS);
        });
        // RetryLater normal akışın parçası (backpressure); her denemeyi ERROR seviyesinde loglamayalım.
        h.setLogLevel(KafkaException.Level.DEBUG);
        return h;
    }

    private static RetryLaterException findRetryLater(Throwable t) {
        while (t != null) {
            if (t instanceof RetryLaterException rl) {
                return rl;
            }
            t = t.getCause();
        }
        return null;
    }
}
