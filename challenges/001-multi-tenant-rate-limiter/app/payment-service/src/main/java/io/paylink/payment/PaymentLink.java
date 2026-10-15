package io.paylink.payment;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentLink(
        String id,
        String tenantId,
        BigDecimal amount,
        String currency,
        String description,
        String url,
        Status status,
        Instant createdAt) {

    public enum Status { PENDING, PAID, EXPIRED }
}
