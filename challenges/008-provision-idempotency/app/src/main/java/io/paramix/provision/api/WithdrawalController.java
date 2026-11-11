package io.paramix.provision.api;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.UUID;

import io.paramix.provision.idempotency.CommandResult;
import io.paramix.provision.withdrawal.WithdrawalService;
import io.paramix.provision.withdrawal.WithdrawalView;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/withdrawals")
public class WithdrawalController {

    public record CreateWithdrawalRequest(@NotBlank String asset, @NotNull @Positive BigDecimal amount,
                                          @NotBlank @Size(max = 200) String destination) {
    }

    private final IdempotentEndpoint idempotent;
    private final WithdrawalService withdrawals;

    public WithdrawalController(IdempotentEndpoint idempotent, WithdrawalService withdrawals) {
        this.idempotent = idempotent;
        this.withdrawals = withdrawals;
    }

    /** 202: istek kabul edildi ve para bloke edildi; zincire gönderim arka planda. Durum GET ile izlenir. */
    @PostMapping
    public ResponseEntity<String> create(@RequestHeader(CustomerContext.HEADER) long customerId,
                                         @RequestHeader(value = CustomerContext.IDEMPOTENCY_HEADER, required = false) String key,
                                         @Valid @RequestBody CreateWithdrawalRequest body) {
        var cmd = new WithdrawalService.RequestWithdrawal(customerId, body.asset().trim().toUpperCase(Locale.ROOT),
                body.amount(), body.destination().trim());
        return idempotent.handle(customerId, key, "withdrawal.create", cmd, () -> {
            WithdrawalView w = withdrawals.request(cmd);
            return CommandResult.accepted(w, w.id());
        });
    }

    @GetMapping("/{id}")
    public WithdrawalView get(@RequestHeader(CustomerContext.HEADER) long customerId, @PathVariable UUID id) {
        return withdrawals.get(customerId, id);
    }
}
