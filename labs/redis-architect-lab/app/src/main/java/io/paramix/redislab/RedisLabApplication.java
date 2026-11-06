package io.paramix.redislab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RedisLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisLabApplication.class, args);
    }
}
