package io.paramix.notify.provider;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.config.ProviderProperties;
import io.paramix.notify.domain.Channel;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.stereotype.Component;

/**
 * Kanal başına sıralı sağlayıcı zinciri (birincil, yedek...) ve sağlayıcı başına circuit breaker.
 * Breaker: son 20 çağrıda hata oranı %30'u ya da 5 sn'den yavaş çağrı oranı %50'yi geçerse açılır,
 * 30 sn sonra birkaç deneme isteğiyle (half-open) kapanmayı dener.
 */
@Component
public class ProviderRegistry {

    private final Map<String, ChannelProvider> byName;
    private final Map<String, ProviderProperties> config;
    private final CircuitBreakerRegistry breakers;

    public ProviderRegistry(NotifyProperties props) {
        this.config = props.providers();
        this.byName = props.providers().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                        e -> new FakeProvider(e.getKey(), e.getValue().channel(), e.getValue().dedupByClientRef())));
        this.breakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(30)
                .slowCallDurationThreshold(Duration.ofSeconds(5))
                .slowCallRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build());
    }

    /** Kanalın sağlayıcıları, order'a göre (1 = birincil). */
    public List<ChannelProvider> chain(Channel channel) {
        return config.entrySet().stream()
                .filter(e -> e.getValue().channel() == channel)
                .sorted(Comparator.comparingInt(e -> e.getValue().order()))
                .map(e -> byName.get(e.getKey()))
                .toList();
    }

    public ChannelProvider get(String name) {
        ChannelProvider p = byName.get(name);
        if (p == null) {
            throw new IllegalArgumentException("Bilinmeyen sağlayıcı: " + name);
        }
        return p;
    }

    public CircuitBreaker breaker(String providerName) {
        return breakers.circuitBreaker(providerName);
    }

    public FakeProvider fake(String name) {
        return (FakeProvider) get(name);
    }
}
