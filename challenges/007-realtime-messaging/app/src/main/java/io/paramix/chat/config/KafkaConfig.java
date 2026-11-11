package io.paramix.chat.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Tek topic: chat.events. Key = sohbet id → bir sohbetin bütün olayları (mesaj, okundu, para isteği durumu)
 * aynı partition'a, yani sırayla aynı fan-out tüketicisine gider. Sıra global değil, sohbet başına önemli.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    NewTopic chatEvents(ChatProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(props.kafka().replicas())
                .build();
    }

    /**
     * Tüketici hatası (ör. Redis'e ulaşılamıyor) → 100 ms'den başlayıp ikiye katlanan aralıklarla, en fazla 5 sn arayla,
     * toplam 30 sn boyunca tekrar dene. Redis failover'ı (~10 sn) bu sürede biter ve olay ATLANMADAN teslim edilir.
     *
     * <p>Spring'in varsayılanı 10 kez beklemeden dener ve olayı atlar: 1 sn'lik bir kesinti bile canlı teslimi ve
     * push'u kaybettiriyordu. Tekrar denerken o partition'daki diğer olaylar bekler (head-of-line); Redis yoksa
     * onlar da zaten teslim edilemiyor. 30 sn sonunda hâlâ olmuyorsa olay loglanıp geçilir: mesaj DB'de, gateway'in
     * RESYNC'i ve istemcinin HEADS kontrolü yakalar.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(ChatProperties props) {
        ExponentialBackOff backOff = new ExponentialBackOff(100, 2.0);
        backOff.setMaxInterval(5_000);
        backOff.setMaxElapsedTime(props.retry().maxElapsed().toMillis());
        return new DefaultErrorHandler((record, e) ->
                log.warn("Olay {} denemelerden sonra atlandı (mesaj DB'de, senkron yakalar): {}", record.key(), e.toString()),
                backOff);
    }
}
