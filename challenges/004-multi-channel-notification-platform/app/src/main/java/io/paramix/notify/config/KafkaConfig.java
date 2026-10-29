package io.paramix.notify.config;

import io.paramix.notify.domain.Topics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.CommonErrorHandler;

@Configuration
public class KafkaConfig {

    /** 3 giriş + 9 teslimat topic'i. Prod'da topic'leri GitOps/Strimzi yönetir; burada lokal kolaylık. */
    @Bean
    KafkaAdmin.NewTopics notificationTopics(NotifyProperties props) {
        NewTopic[] topics = Topics.all().stream()
                .map(name -> TopicBuilder.name(name)
                        .partitions(props.kafka().partitions())
                        .replicas(props.kafka().replicas())
                        .build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }

    /** Spring Boot bu bean'i @KafkaListener container'larına (router) otomatik bağlar. */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        return KafkaErrorHandlers.create();
    }
}
