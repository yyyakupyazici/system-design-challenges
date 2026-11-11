package io.paramix.provision.api;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.domain.BalanceRepository;
import io.paramix.provision.domain.DomainException;
import io.paramix.provision.domain.ErrorCode;
import io.paramix.provision.domain.Provision;
import io.paramix.provision.domain.Provision.OperationType;
import io.paramix.provision.domain.ProvisionService;
import io.paramix.provision.domain.ProvisionView;
import io.paramix.provision.idempotency.CommandResult;

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
@RequestMapping("/v1/provisions")
public class ProvisionController {

    public record CreateProvisionRequest(@NotBlank String asset, @NotNull @Positive BigDecimal amount,
                                         @NotNull OperationType operationType,
                                         @NotBlank @Size(max = 128) String operationRef,
                                         @Positive Long ttlSeconds) {
    }

    public record CaptureRequest(@Positive BigDecimal amount) {
    }

    /** Parmak izine giren komutlar: path'teki id de isteğin anlamının parçası. */
    private record CaptureCommand(UUID provisionId, BigDecimal amount) {
    }

    private record ReleaseCommand(UUID provisionId) {
    }

    private final IdempotentEndpoint idempotent;
    private final ProvisionService provisions;
    private final BalanceRepository balances;

    public ProvisionController(IdempotentEndpoint idempotent, ProvisionService provisions, BalanceRepository balances) {
        this.idempotent = idempotent;
        this.provisions = provisions;
        this.balances = balances;
    }

    @PostMapping
    public ResponseEntity<String> create(@RequestHeader(CustomerContext.HEADER) long customerId,
                                         @RequestHeader(value = CustomerContext.IDEMPOTENCY_HEADER, required = false) String key,
                                         @Valid @RequestBody CreateProvisionRequest body) {
        var cmd = new ProvisionService.HoldCommand(customerId, body.asset().trim().toUpperCase(Locale.ROOT),
                body.amount(), body.operationType(), body.operationRef(),
                body.ttlSeconds() == null ? null : Duration.ofSeconds(body.ttlSeconds()));
        return idempotent.handle(customerId, key, "provision.create", cmd, () -> {
            if (cmd.operationType() == OperationType.WITHDRAWAL) {
                throw new DomainException(ErrorCode.OPERATION_TYPE_NOT_ALLOWED,
                        "Çekim provizyonu POST /v1/withdrawals ile açılır", Map.of("operationType", "WITHDRAWAL"));
            }
            Provision p = provisions.hold(cmd);
            return CommandResult.created(provisions.view(p), p.id());
        });
    }

    @GetMapping("/{id}")
    public ProvisionView get(@RequestHeader(CustomerContext.HEADER) long customerId, @PathVariable UUID id) {
        return provisions.view(provisions.get(customerId, id));
    }

    @PostMapping("/{id}/capture")
    public ResponseEntity<String> capture(@RequestHeader(CustomerContext.HEADER) long customerId,
                                          @RequestHeader(value = CustomerContext.IDEMPOTENCY_HEADER, required = false) String key,
                                          @PathVariable UUID id,
                                          @Valid @RequestBody(required = false) CaptureRequest body) {
        BigDecimal amount = body == null ? null : body.amount();
        return idempotent.handle(customerId, key, "provision.capture", new CaptureCommand(id, amount), () -> {
            Provision p = provisions.capture(customerId, id, amount);
            return CommandResult.ok(provisions.view(p), p.id());
        });
    }

    @PostMapping("/{id}/release")
    public ResponseEntity<String> release(@RequestHeader(CustomerContext.HEADER) long customerId,
                                          @RequestHeader(value = CustomerContext.IDEMPOTENCY_HEADER, required = false) String key,
                                          @PathVariable UUID id) {
        return idempotent.handle(customerId, key, "provision.release", new ReleaseCommand(id), () -> {
            Provision p = provisions.release(customerId, id);
            return CommandResult.ok(provisions.view(p), p.id());
        });
    }
}
