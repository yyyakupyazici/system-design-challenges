package io.paramix.provision.kafka;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import io.paramix.provision.trade.TradeEvent;
import io.paramix.provision.trade.TradeEventHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * {@code trade.events} dinleyicisi. İnce tutuldu: mesajı çözer, kuralı {@link TradeEventHandler}'a bırakır.
 *
 * <p>Offset, handler'ın DB transaction'ı commit edildikten SONRA commit edilir (ack-mode RECORD). Aradaki çökme
 * mesajı tekrar getirir, inbox onu yakalar. Ters sıra (önce offset, sonra DB) mesaj KAYBI demek olurdu.
 */
public class TradeEventsListener {

    private final TradeEventHandler handler;
    private final ObjectMapper json;

    public TradeEventsListener(TradeEventHandler handler, ObjectMapper json) {
        this.handler = handler;
        this.json = json;
    }

    @KafkaListener(id = "trade-events", topics = "${provision.kafka.topics.trade-events}",
            concurrency = "${provision.kafka.consumer-concurrency:3}")
    public void onMessage(ConsumerRecord<String, String> record) {
        handler.handle(eventId(record), parse(record));
    }

    private UUID eventId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(KafkaEventPublisher.HEADER_EVENT_ID);
        if (header == null) {
            // Offset'e göre tekilleştirmek yanlış olurdu: aynı olay yeniden üretilirse farklı offset alır.
            throw new InvalidEventException("event_id header'ı yok (" + record.topic() + "-" + record.partition()
                    + "@" + record.offset() + ")", null);
        }
        try {
            return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("event_id UUID değil", e);
        }
    }

    private TradeEvent parse(ConsumerRecord<String, String> record) {
        try {
            TradeEvent event = json.readValue(record.value(), TradeEvent.class);
            if (event.type() == null || event.orderId() == null || event.customerId() <= 0) {
                throw new InvalidEventException("Eksik alan: " + record.value(), null);
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Çözülemeyen trade olayı", e);
        }
    }
}
