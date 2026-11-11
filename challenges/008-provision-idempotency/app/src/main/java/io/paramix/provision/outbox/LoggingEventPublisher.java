package io.paramix.provision.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Demo yayıncısı. Gerçek ortamda Kafka adaptörü (key = aggregateId: bir provizyonun olayları sırayla) takılır. */
@Configuration
public class LoggingEventPublisher {

    private static final Logger log = LoggerFactory.getLogger("events");

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    public EventPublisher loggingPublisher() {
        return event -> log.info("{} {} {} {}", event.eventType(), event.aggregateId(), event.eventId(), event.payload());
    }
}
