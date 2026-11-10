package io.paramix.chat.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Tek topic: chat.events. Key = sohbet id → bir sohbetin bütün olayları (mesaj, okundu, para isteği durumu)
 * aynı partition'a, yani sırayla aynı fan-out tüketicisine gider. Sıra global değil, sohbet başına önemli.
 */
@Configuration
public class KafkaConfig {

    @Bean
    NewTopic chatEvents(ChatProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(props.kafka().replicas())
                .build();
    }
}
