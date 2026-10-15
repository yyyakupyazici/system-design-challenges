package io.paylink.payment;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("paylink")
public record PaymentProperties(
        @DefaultValue("20ms") Duration simulatedDbLatency,
        @DefaultValue("https://pay.paylink.io/l/") String publicLinkBaseUrl) {
}
