package io.paramix.payreq.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    /** Bu servisin sahibi olduğu topic. Key = para isteği id → bir isteğin olayları sırayla. */
    @Bean
    NewTopic paymentEvents(PayReqProperties props) {
        return TopicBuilder.name(props.topics().events()).partitions(props.topics().partitions())
                .replicas(props.topics().replicas()).build();
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(100, 2.0);
        backOff.setMaxInterval(5_000);
        backOff.setMaxElapsedTime(60_000);
        return new DefaultErrorHandler((r, e) -> log.error("Cüzdan olayı işlenemedi, atlandı (reconciler yakalar): {}", e.toString()),
                backOff);
    }
}
