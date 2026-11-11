package io.paramix.provision.kafka;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.outbox.EventPublisher;
import io.paramix.provision.trade.TradeEventHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Kafka sadece {@code provision.kafka.enabled=true} iken devrede. Kapalıyken olaylar loga yazılır ve hiçbir şey
 * Kafka'ya bağlanmaz (testler ve Kafka'sız lokal geliştirme).
 */
@Configuration
@ConditionalOnProperty(name = "provision.kafka.enabled", havingValue = "true")
public class KafkaConfig {

    public static final String DLT_SUFFIX = ".DLT";

    @Bean
    public EventPublisher kafkaEventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json,
                                              ProvisionProperties props, KafkaProperties kafkaProps) {
        // Onay beklemesi producer'ın kendi teslim süresine bağlı: ondan önce vazgeçmek, Kafka'ya ulaşmış bir
        // partiyi "başarısız" sayıp gereksiz tekrar üretir.
        long deliveryTimeout = Long.parseLong(
                kafkaProps.getProducer().getProperties().getOrDefault("delivery.timeout.ms", "15000"));
        return new KafkaEventPublisher(kafka, json, props, deliveryTimeout + 1_000);
    }

    @Bean
    public TradeEventsListener tradeEventsListener(TradeEventHandler handler, ObjectMapper json) {
        return new TradeEventsListener(handler, json);
    }

    /**
     * Geçici hata (DB kısa süre yok, provizyon henüz görünmüyor): artan aralıklarla ~10 sn tekrar dene.
     * Kalıcı hata (bozuk mesaj): hiç bekleme, doğrudan {@code <topic>.DLT}.
     * DLT'ye giden mesaj partition'ı tıkamaz; aynı partition'daki sonraki emirler işlenmeye devam eder.
     * Çelişkiler (CONFLICT) buraya hiç gelmez: inbox'a kaydedilip mesaj tüketilir.
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        ExponentialBackOff backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(3_000);
        backOff.setMaxElapsedTime(10_000);
        // Hedef açıkça yazılı: spring-kafka'nın varsayılan DLT son eki sürümler arasında değişti (".DLT" → "-dlt").
        // Varsayılana güvenmek, bir sürüm yükseltmesinde ölü mesajları kimsenin izlemediği bir topic'e yollar.
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }

    @Bean
    public NewTopic provisionEventsTopic(ProvisionProperties props) {
        var t = props.kafka().topics();
        return TopicBuilder.name(t.provisionEvents()).partitions(t.partitions()).replicas(t.replicas()).build();
    }

    /** trade.events'in sahibi emir motoru; demo ve testler için burada da tanımlı (varsa dokunulmaz). */
    @Bean
    public NewTopic tradeEventsTopic(ProvisionProperties props) {
        var t = props.kafka().topics();
        return TopicBuilder.name(t.tradeEvents()).partitions(t.partitions()).replicas(t.replicas()).build();
    }

    /** DLT, kaynak topic ile aynı partition sayısında: recoverer mesajı aynı partition numarasına yazar. */
    @Bean
    public NewTopic tradeEventsDltTopic(ProvisionProperties props) {
        var t = props.kafka().topics();
        return TopicBuilder.name(t.tradeEvents() + DLT_SUFFIX).partitions(t.partitions()).replicas(t.replicas()).build();
    }
}
