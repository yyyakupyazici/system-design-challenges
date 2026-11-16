package io.paramix.raftlab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Bir "düğüm ajanı" (mini-Patroni). Her PostgreSQL'in yanında bir tane çalışır:
 * etcd'deki lider kaydına bakar, liderse süresini uzatır, liderliği kaybederse PostgreSQL'i yazmaya kapatır,
 * lider yoksa ve uygunsa lider olur ve PostgreSQL'i promote eder.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class RaftLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(RaftLabApplication.class, args);
    }
}
