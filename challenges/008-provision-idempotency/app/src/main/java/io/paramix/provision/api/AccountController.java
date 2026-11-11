package io.paramix.provision.api;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

import io.paramix.provision.domain.AssetCatalog;
import io.paramix.provision.domain.BalanceRepository;
import io.paramix.provision.domain.DepositService;
import io.paramix.provision.reconciliation.Reconciliation;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountController {

    public record BalanceView(String asset, String available, String held, String total) {
    }

    public record DepositRequest(@NotNull @Positive Long customerId, @NotBlank String asset,
                                 @NotNull @Positive BigDecimal amount, @NotNull DepositService.Source source,
                                 @NotBlank @Size(max = 200) String externalRef) {
    }

    private final BalanceRepository balances;
    private final AssetCatalog assets;
    private final DepositService deposits;
    private final Reconciliation reconciliation;

    public AccountController(BalanceRepository balances, AssetCatalog assets, DepositService deposits,
                             Reconciliation reconciliation) {
        this.balances = balances;
        this.assets = assets;
        this.deposits = deposits;
        this.reconciliation = reconciliation;
    }

    @GetMapping("/v1/balances")
    public List<BalanceView> balances(@RequestHeader(CustomerContext.HEADER) long customerId) {
        return balances.findByCustomer(customerId).stream().map(b -> {
            var a = assets.require(b.asset());
            return new BalanceView(b.asset(), a.format(b.available()), a.format(b.held()),
                    a.format(b.available().add(b.held())));
        }).toList();
    }

    /**
     * İç uç: banka/zincir entegrasyonu çağırır (ağ politikasıyla dışarı kapalı). Idempotency doğal anahtarla:
     * 201 = yeni yazıldı, 200 = bu yatırım zaten vardı (tekrar gelen bildirim).
     */
    @PostMapping("/internal/deposits")
    public ResponseEntity<DepositService.DepositView> deposit(@Valid @RequestBody DepositRequest body) {
        var result = deposits.record(new DepositService.RecordDeposit(body.customerId(),
                body.asset().trim().toUpperCase(Locale.ROOT), body.amount(), body.source(), body.externalRef().trim()));
        return ResponseEntity.status(result.created() ? 201 : 200).body(result.deposit());
    }

    @GetMapping("/internal/reconciliation")
    public List<Reconciliation.Discrepancy> reconcile() {
        return reconciliation.run();
    }
}
