package io.paylink.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("ratelimit")
public record RateLimitProperties(
        @DefaultValue("true") boolean failOpen,
        @DefaultValue("50ms") Duration redisTimeout,
        @DefaultValue("P2D") Duration quotaKeyTtl,
        @DefaultValue("PT60S") Duration policyRefreshInterval,
        @DefaultValue Global global) {

    /** Tüm sistemi koruyan global kova (gereksinim 4: backend toplam kapasitesi). */
    public record Global(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("25000") int ratePerSec,
            @DefaultValue("25000") int burst) {
    }
}
