package io.paylink.gateway.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("paylink")
public record PaylinkProperties(
        @NotBlank String paymentServiceUrl,
        @NotBlank String adminToken) {
}
