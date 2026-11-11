package io.paramix.provision.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.outbox.EventPublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Outbox olaylarını Kafka'ya yazar.
 *
 * <ul>
 *   <li><b>Key = aggregateId:</b> bir provizyonun (ya da çekimin) bütün olayları aynı partition'a, yani sırayla gider.
 *       held → captured sırası tüketicide bozulmaz.</li>
 *   <li><b>Idempotent producer</b> ({@code enable.idempotence=true}, {@code acks=all}): ağ hatasında producer'ın
 *       kendi tekrarı Kafka'da kopya ya da sıra bozulması üretmez. Ama relay'in TEKRARINI (outbox satırı silinemeden
 *       çökme) engellemez; o yüzden her mesaj {@code event_id} taşır ve tüketici inbox ile tekilleştirir.</li>
 *   <li><b>Zarf (envelope):</b> değer kendini tanımlar ({@code eventId}, {@code eventType}, ...); aynı bilgiler
 *       header'da da var. Header'ları kaybeden araçlar (bazı connector'lar, elle replay) mesajı yine işleyebilir.</li>
 *   <li><b>Parti senkron:</b> önce hepsi gönderilir, sonra hepsinin onayı beklenir. Biri bile başarısızsa istisna:
 *       relay transaction'ı geri alır, hiçbir satır silinmez.</li>
 * </ul>
 */
public class KafkaEventPublisher implements EventPublisher {

    public static final String HEADER_EVENT_ID = "event_id";
    public static final String HEADER_EVENT_TYPE = "event_type";
    public static final String HEADER_AGGREGATE_TYPE = "aggregate_type";

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String topic;
    private final long ackTimeoutMillis;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json, ProvisionProperties props,
                               long ackTimeoutMillis) {
        this.kafka = kafka;
        this.json = json;
        this.topic = props.kafka().topics().provisionEvents();
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    @Override
    public void publish(List<OutboxEvent> batch) {
        List<CompletableFuture<SendResult<String, String>>> acks = new ArrayList<>(batch.size());
        for (OutboxEvent event : batch) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, event.aggregateId().toString(), envelope(event));
            record.headers()
                    .add(HEADER_EVENT_ID, bytes(event.eventId().toString()))
                    .add(HEADER_EVENT_TYPE, bytes(event.eventType()))
                    .add(HEADER_AGGREGATE_TYPE, bytes(event.aggregateType()));
            acks.add(kafka.send(record));
        }
        kafka.flush();
        try {
            for (CompletableFuture<SendResult<String, String>> ack : acks) {
                ack.get(ackTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka yayını onaylanmadı; parti tekrar gönderilecek", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka yayını kesildi", e);
        }
    }

    private String envelope(OutboxEvent event) {
        try {
            ObjectNode env = json.createObjectNode();
            env.put("eventId", event.eventId().toString());
            env.put("eventType", event.eventType());
            env.put("aggregateType", event.aggregateType());
            env.put("aggregateId", event.aggregateId().toString());
            env.put("occurredAt", event.occurredAt().toString());
            env.set("data", json.readTree(event.payload()));
            return json.writeValueAsString(env);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Outbox payload JSON değil: " + event.eventId(), e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
