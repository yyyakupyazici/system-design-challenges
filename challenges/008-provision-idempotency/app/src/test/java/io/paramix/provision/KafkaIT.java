package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.support.UuidV7;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.kafka.KafkaContainer;

/** Gerçek Kafka (Testcontainers) ile uçtan uca: outbox → provision.events ve trade.events → inbox → provizyon. */
@DisplayName("Kafka: outbox yayını, inbox tüketimi, DLT")
@TestPropertySource(properties = {
        "provision.kafka.enabled=true",
        "provision.outbox.relay-enabled=true",
        "provision.outbox.interval=100ms",
        "provision.kafka.topics.partitions=3"
})
class KafkaIT extends IntegrationTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    private static final long ALICE = 5001;
    private static final ObjectMapper JSON = new ObjectMapper();

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void clients() {
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }

    @AfterEach
    void closeClients() {
        producer.close();
        consumer.close();
    }

    @Test
    @DisplayName("Outbox → provision.events: key = provizyon id, sıra korunur, event_id header'ı ve zarf")
    void outboxEventsReachKafkaInOrderWithEventId() {
        fund(ALICE, "TRY", "1000");
        String id = api.createProvision(ALICE, newKey(), "TRY", "400", newRef()).field("id");
        api.capture(ALICE, newKey(), id, "350");

        consumer.subscribe(List.of("provision.events"));
        List<ConsumerRecord<String, String>> mine = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                if (id.equals(r.key())) {
                    mine.add(r);
                }
            });
            return mine.size() >= 2;
        });

        assertThat(mine).extracting(r -> header(r, "event_type")).containsExactly("provision.held", "provision.captured");
        assertThat(mine).extracting(ConsumerRecord::partition).containsOnly(mine.getFirst().partition());
        JsonNode envelope = readJson(mine.get(1).value());
        assertThat(envelope.get("eventId").asText()).isEqualTo(header(mine.get(1), "event_id"));
        assertThat(envelope.at("/data/provision/capturedAmount").asText()).isEqualTo("350.00");
        await().atMost(Duration.ofSeconds(5)).until(() -> count("SELECT count(*) FROM outbox") == 0);
    }

    @Test
    @DisplayName("trade.events: aynı dolum olayı 3 kez yayınlanır → provizyon bir kez capture, inbox'ta tek kayıt")
    void duplicatedTradeEventIsAppliedOnce() {
        fund(ALICE, "USDT", "1000");
        String order = newRef();
        api.createProvision(ALICE, newKey(), "USDT", "600", order);
        String eventId = UuidV7.generate(Instant.now()).toString();
        String fill = """
                {"type":"ORDER_FILLED","customerId":%d,"orderId":"%s","filledAmount":"550"}""".formatted(ALICE, order);

        for (int i = 0; i < 3; i++) {
            send("trade.events", order, fill, eventId);
        }

        await().atMost(Duration.ofSeconds(20)).until(() -> held(ALICE, "USDT").signum() == 0);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                .until(() -> count("SELECT count(*) FROM inbox") == 1);
        assertThat(available(ALICE, "USDT")).isEqualByComparingTo("450");
    }

    @Test
    @DisplayName("Bozuk mesaj (event_id yok) tekrar denenmeden DLT'ye gider, partition tıkanmaz")
    void malformedMessageGoesToDltWithoutBlockingThePartition() {
        fund(ALICE, "TRY", "100");
        String order = newRef();
        api.createProvision(ALICE, newKey(), "TRY", "100", order);

        long start = System.nanoTime();
        send("trade.events", order, """
                {"type":"ORDER_CANCELLED","customerId":%d,"orderId":"%s"}""".formatted(ALICE, order), null);
        send("trade.events", order, """
                {"type":"ORDER_CANCELLED","customerId":%d,"orderId":"%s"}""".formatted(ALICE, order),
                UuidV7.generate(Instant.now()).toString());

        consumer.subscribe(List.of("trade.events.DLT"));
        List<ConsumerRecord<String, String>> dead = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                if (order.equals(r.key())) {
                    dead.add(r);
                }
            });
            return !dead.isEmpty();
        });
        assertThat(header(dead.getFirst(), "kafka_dlt-exception-cause-fqcn"))
                .isEqualTo("io.paramix.provision.kafka.InvalidEventException");
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .as("kalıcı hata: ~10 sn'lik tekrar denemeye girmeden DLT'ye gitti").isLessThan(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(20)).until(() -> held(ALICE, "TRY").signum() == 0);   // arkasındaki işlendi
    }

    private void send(String topic, String key, String value, String eventId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        if (eventId != null) {
            record.headers().add("event_id", eventId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String header(ConsumerRecord<String, String> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static JsonNode readJson(String s) {
        try {
            return JSON.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
