package io.paramix.notify.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.paramix.notify.store.OutboxRepository;
import io.paramix.notify.store.OutboxRepository.OutboxRow;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox → Kafka. API, bildirimi ve outbox satırını aynı DB transaction'ında yazar; bu süreç satırları
 * Kafka'ya taşır. Kafka'ya yazıp published_at'i işaretleyemeden çökerse satır tekrar gönderilir
 * (at-least-once) — router deterministik delivery_id ile bunu zararsız kılar.
 * Prod alternatifi: Debezium (CDC) ile WAL'dan okumak.
 */
@Component
@ConditionalOnProperty(name = "notify.roles.scheduler", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka) {
        this.outbox = outbox;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "${notify.scheduler.outbox-ms:200}")
    @Transactional
    public void relay() throws Exception {
        List<OutboxRow> rows = outbox.lockBatch(500);
        if (rows.isEmpty()) {
            return;
        }
        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>();
        for (OutboxRow row : rows) {
            sends.add(kafka.send(row.topic(), row.key(), row.payload()));
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        outbox.markPublished(rows.stream().map(OutboxRow::id).toList());
    }
}
