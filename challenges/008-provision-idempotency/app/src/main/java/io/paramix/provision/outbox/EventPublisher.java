package io.paramix.provision.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Outbox olaylarının gideceği yer (Kafka, SNS...). Uygulama en az bir kez (at-least-once) teslim eder; tüketici
 * {@code eventId} ile kendi inbox tablosunda tekilleştirir.
 */
public interface EventPublisher {

    record OutboxEvent(UUID eventId, String aggregateType, UUID aggregateId, String eventType, String payload,
                       Instant occurredAt) {
    }

    /**
     * Partinin TAMAMI yayınlanıp onaylanınca döner; biri bile başarısızsa istisna fırlatır ve relay hiçbir satırı
     * silmez (parti tekrar gönderilir, tüketici event_id ile tekilleştirir).
     */
    void publish(List<OutboxEvent> batch);
}
