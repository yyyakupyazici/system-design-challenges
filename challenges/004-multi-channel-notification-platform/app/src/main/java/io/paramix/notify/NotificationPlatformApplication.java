package io.paramix.notify;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Tek imaj, dört rol: {@code notify.roles.api|router|worker|scheduler}.
 * Kubernetes'te her rol ayrı Deployment olarak ölçeklenir; lokal geliştirmede hepsi tek süreçte çalışır.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class NotificationPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationPlatformApplication.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
