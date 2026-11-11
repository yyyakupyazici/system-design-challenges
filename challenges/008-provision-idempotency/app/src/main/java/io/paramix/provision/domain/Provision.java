package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Provision(UUID id, long customerId, String asset, BigDecimal amount, BigDecimal capturedAmount,
                        OperationType operationType, String operationRef, Status status, Instant expiresAt,
                        Instant createdAt, Instant updatedAt) {

    public enum Status { HELD, CAPTURED, RELEASED, EXPIRED }

    public enum OperationType {
        /** Al-sat emri: süreli. Emir dolunca capture, iptalde release, unutulursa EXPIRE. */
        TRADE,
        /** Çekim: süresiz. Para zincire/bankaya gitmiş olabilir; sadece çekim süreci kapatır. */
        WITHDRAWAL,
        /** İç transfer. */
        TRANSFER
    }
}
