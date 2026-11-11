package io.paramix.payreq.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("payreq")
public record PayReqProperties(Duration requestTtl, Duration reconcileAfter, Topics topics) {

    public record Topics(String events, String wallet, int partitions, int replicas) {
    }
}
