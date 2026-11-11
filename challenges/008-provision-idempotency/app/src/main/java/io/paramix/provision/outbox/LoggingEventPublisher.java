package io.paramix.provision.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Kafka kapalıyken (provision.kafka.enabled=false) olayları loga yazar. Açıkken KafkaEventPublisher devreye girer. */
@Configuration
public class LoggingEventPublisher {

    private static final Logger log = LoggerFactory.getLogger("events");

    // @ConditionalOnMissingBean değil: kullanıcı config sınıfları arasında değerlendirme sırası garanti değil.
    @Bean
    @ConditionalOnProperty(name = "provision.kafka.enabled", havingValue = "false", matchIfMissing = true)
    public EventPublisher loggingPublisher() {
        return batch -> batch.forEach(event ->
                log.info("{} {} {} {}", event.eventType(), event.aggregateId(), event.eventId(), event.payload()));
    }
}
