package io.paramix.notify.config;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.Priority;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("notify")
public record NotifyProperties(
        @DefaultValue Roles roles,
        @DefaultValue Kafka kafka,
        @DefaultValue Worker worker,
        @DefaultValue Delivery delivery,
        @DefaultValue Quota quota,
        Map<String, ProviderProperties> providers) {

    public NotifyProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
    }

    /** Hangi rollerin bu süreçte çalışacağı. Lokal: hepsi; Kubernetes: Deployment başına bir rol. */
    public record Roles(@DefaultValue("true") boolean api,
                        @DefaultValue("true") boolean router,
                        @DefaultValue("true") boolean worker,
                        @DefaultValue("true") boolean scheduler) {
    }

    public record Kafka(@DefaultValue("12") int partitions,
                        @DefaultValue("1") short replicas) {
    }

    /** Worker rolü hangi kanalları işler (ör. SMS pod'ları sadece SMS) ve öncelik başına kaç consumer thread'i. */
    public record Worker(@DefaultValue({"SMS", "EMAIL", "PUSH"}) List<Channel> channels,
                         Map<Priority, Integer> concurrency) {

        public Worker {
            Map<Priority, Integer> c = new EnumMap<>(Priority.class);
            c.put(Priority.CRITICAL, 4);
            c.put(Priority.TRANSACTIONAL, 4);
            c.put(Priority.MARKETING, 2);
            if (concurrency != null) {
                c.putAll(concurrency);
            }
            concurrency = Map.copyOf(c);
        }
    }

    /** lease, en uzun kota beklemesinden (marketing 30 sn) + sağlayıcı timeout'undan uzun olmalı. */
    public record Delivery(@DefaultValue("60s") Duration lease,
                           @DefaultValue("10s") Duration fallbackWait,
                           @DefaultValue("5") int maxAttempts,
                           @DefaultValue("30s") Duration unknownGrace) {
    }

    public record Quota(@DefaultValue("10") int fallbackPodCount,
                        @DefaultValue("60s") Duration keyTtl,
                        @DefaultValue("0.7") double aimdDecrease,
                        @DefaultValue("50") int aimdIncreaseStep,
                        @DefaultValue("30s") Duration aimdRecoverAfter) {
    }
}
