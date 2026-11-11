package io.paramix.provision.outbox;

import java.util.UUID;

/**
 * Outbox olaylarının gideceği yer (Kafka, SNS...). Uygulama en az bir kez (at-least-once) teslim eder; tüketici
 * {@code eventId} ile kendi inbox tablosunda tekilleştirir.
 */
public interface EventPublisher {

    record OutboxEvent(UUID eventId, String aggregateType, UUID aggregateId, String eventType, String payload) {
    }

    void publish(OutboxEvent event);
}
