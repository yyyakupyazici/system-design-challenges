package io.paramix.provision.withdrawal;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Withdrawal(UUID id, long customerId, String asset, BigDecimal amount, String destination,
                         UUID provisionId, Status status, String txHash, String signedPayload, UUID claimToken,
                         int failures, String lastError, Instant createdAt, Instant updatedAt) {

    public enum Status {
        PENDING, SIGNED, BROADCAST, CONFIRMED, FAILED, MANUAL_REVIEW;

        public boolean terminal() {
            return this == CONFIRMED || this == FAILED || this == MANUAL_REVIEW;
        }
    }
}
