package io.paramix.notify.messaging;

import java.util.concurrent.TimeUnit;

import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.Topics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Teslimatı {@code <kanal>.<öncelik>} topic'ine yazar. Key = userId: aynı kullanıcının bildirimleri
 * aynı partition'a düşer ve sırayla işlenir ("kart bloke" → "kart aktif", zorluk #4).
 * Senkron bekler: offset ancak mesaj Kafka'ya yazıldıktan sonra commit edilsin.
 */
@Component
public class DeliveryPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;

    public DeliveryPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    public void publish(DeliveryMessage m) {
        try {
            kafka.send(Topics.delivery(m.channel(), m.priority()), m.userId(), json.writeValueAsString(m))
                    .get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Kafka'ya yazılamadı: " + m.deliveryId(), e);
        }
    }
}
