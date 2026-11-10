package io.paramix.chat.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("chat")
public record ChatProperties(Roles roles, Gateway gateway, Sync sync, Kafka kafka,
                             @DefaultValue("30s") Duration membersCacheTtl,
                             Retention retention, Payment payment) {

    public record Roles(boolean gateway, boolean relay, boolean fanout, boolean payment) {
    }

    public record Gateway(String name, Duration heartbeat, Duration lease, int maxConnections, int handshakesPerSecond,
                          Duration drainSpread, Duration sendTimeLimit, int sendBufferLimit) {
    }

    public record Sync(int pageSize, int maxConcurrent, int newDeviceHistory) {
    }

    public record Kafka(String topic, int partitions, int replicas) {
    }

    public record Retention(Duration messages, String cron) {
    }

    public record Payment(Duration requestTtl, Duration reconcileAfter) {
    }
}
