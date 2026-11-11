package io.paramix.provision.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

    /** Bütün iş zamanı UTC. Gün sınırı (partition günü) da UTC'ye göre hesaplanır. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
