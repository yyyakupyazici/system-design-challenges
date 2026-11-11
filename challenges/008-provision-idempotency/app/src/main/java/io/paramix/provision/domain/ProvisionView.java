package io.paramix.provision.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Provizyonun dışarıya görünen hali (API cevabı ve olay gövdesi). Tutarlar STRING: JSON number'ı JavaScript'te
 * double'a dönüşür, 0.1 + 0.2 ≠ 0.3 olur, 18 basamaklı ETH tutarı bozulur.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProvisionView(UUID id, String asset, String amount, String capturedAmount, String releasedAmount,
                            Provision.OperationType operationType, String operationRef, Provision.Status status,
                            Instant expiresAt, Instant createdAt, Instant updatedAt) {

    public static ProvisionView of(Provision p, Asset asset) {
        String released = switch (p.status()) {
            case CAPTURED -> asset.format(p.amount().subtract(p.capturedAmount()));
            case RELEASED, EXPIRED -> asset.format(p.amount());
            case HELD -> null;
        };
        return new ProvisionView(p.id(), p.asset(), asset.format(p.amount()), asset.format(p.capturedAmount()),
                released, p.operationType(), p.operationRef(), p.status(), p.expiresAt(), p.createdAt(), p.updatedAt());
    }
}
