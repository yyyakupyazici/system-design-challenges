package io.paramix.provision.withdrawal;

import java.time.Instant;
import java.util.UUID;

import io.paramix.provision.domain.Asset;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record WithdrawalView(UUID id, String asset, String amount, String destination, Withdrawal.Status status,
                             String txHash, UUID provisionId, Instant createdAt, Instant updatedAt) {

    public static WithdrawalView of(Withdrawal w, Asset asset) {
        return new WithdrawalView(w.id(), w.asset(), asset.format(w.amount()), w.destination(), w.status(), w.txHash(),
                w.provisionId(), w.createdAt(), w.updatedAt());
    }
}
