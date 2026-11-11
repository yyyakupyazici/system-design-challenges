package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.domain.Ledger.Bucket;
import io.paramix.provision.domain.Ledger.EntryType;
import io.paramix.provision.domain.Provision.OperationType;
import io.paramix.provision.domain.Provision.Status;
import io.paramix.provision.outbox.Outbox;
import io.paramix.provision.support.UuidV7;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Provizyonun bütün kuralları. Her metot tek bir transaction'ın içinde çalışır ve şu dördünü birlikte yazar:
 * provizyon satırı, bakiye satırı, ledger journal'ı, outbox olayı. Biri yazılıp diğeri yazılamaz.
 *
 * <p>İki seviyede idempotency:
 * <ul>
 *   <li><b>API</b> (Idempotency-Key): aynı HTTP isteğinin tekrarı → aynı cevap. Bkz. IdempotentExecutor.</li>
 *   <li><b>İş</b> (bu sınıf): aynı emir için ikinci provizyon, FARKLI bir key ile gelse bile açılmaz
 *       (UNIQUE customer_id, operation_type, operation_ref). İstemci key'i kaybetse, uygulama yeniden kurulsa,
 *       emir servisi kendi retry'ını yapsa bile tek emir = tek provizyon.</li>
 * </ul>
 * Durum geçişleri de kendi başına idempotenttir: koşullu UPDATE ikinci kez hiçbir şey değiştirmez.
 */
@Service
public class ProvisionService {

    public record HoldCommand(long customerId, String asset, BigDecimal amount, OperationType operationType,
                              String operationRef, Duration ttl) {
    }

    private final ProvisionRepository provisions;
    private final BalanceRepository balances;
    private final Ledger ledger;
    private final Outbox outbox;
    private final AssetCatalog assets;
    private final Clock clock;
    private final ProvisionProperties.Holds holdProps;
    private final MeterRegistry meters;

    public ProvisionService(ProvisionRepository provisions, BalanceRepository balances, Ledger ledger, Outbox outbox,
                            AssetCatalog assets, Clock clock, ProvisionProperties props, MeterRegistry meters) {
        this.provisions = provisions;
        this.balances = balances;
        this.ledger = ledger;
        this.outbox = outbox;
        this.assets = assets;
        this.clock = clock;
        this.holdProps = props.holds();
        this.meters = meters;
    }

    @Transactional
    public Provision hold(HoldCommand cmd) {
        Asset asset = assets.require(cmd.asset());
        BigDecimal amount = asset.requireValidAmount(cmd.amount());
        Instant expiresAt = expiryFor(cmd.operationType(), cmd.ttl());
        UUID id = UuidV7.generate(clock);

        // Önce provizyon satırı: aynı emir için eşzamanlı ikinci istek unique indekste bekler, ilki commit edince
        // çakışmayı görür. Bakiye düşümü bundan SONRA, yani kaybeden istek bakiyeye hiç dokunmaz.
        if (!provisions.insertHeld(id, cmd.customerId(), asset.code(), amount, cmd.operationType(),
                cmd.operationRef(), expiresAt)) {
            Provision existing = provisions.findByOperation(cmd.customerId(), cmd.operationType(), cmd.operationRef())
                    .orElseThrow();
            throw new DomainException(ErrorCode.PROVISION_ALREADY_EXISTS,
                    cmd.operationType() + " " + cmd.operationRef() + " için provizyon zaten açılmış",
                    Map.of("provisionId", existing.id(), "status", existing.status()));
        }
        if (!balances.hold(cmd.customerId(), asset.code(), amount)) {
            throw new DomainException(ErrorCode.INSUFFICIENT_FUNDS,
                    asset.code() + " kullanılabilir bakiyesi " + asset.format(amount) + " için yetmiyor",
                    Map.of("asset", asset.code(), "requested", asset.format(amount)));
        }
        ledger.journal(cmd.customerId(), asset.code(), EntryType.HOLD, id)
                .move(Bucket.AVAILABLE, Bucket.HELD, amount)
                .post();
        Provision held = provisions.findById(id).orElseThrow();
        publish(held, "provision.held");
        meters.counter("provisions", "transition", "held", "asset", asset.code()).increment();
        return held;
    }

    /**
     * @param actingCustomerId API'den gelen çağrıda çağıranın müşteri id'si (başkasının provizyonuna dokunamaz);
     *                         iç süreçlerde (çekim worker'ı) null.
     * @param amount           null = tamamı. Daha azı = kısmi capture, kalan otomatik serbest kalır
     *                         (ör. 1.000 TRY bloke edilen alım emri 980 TRY'ye doldu).
     */
    @Transactional
    public Provision capture(Long actingCustomerId, UUID provisionId, BigDecimal amount) {
        Provision current = load(actingCustomerId, provisionId);
        Asset asset = assets.require(current.asset());
        BigDecimal captured = amount == null ? current.amount() : asset.requireValidAmount(amount);
        if (captured.compareTo(current.amount()) > 0) {
            throw new DomainException(ErrorCode.CAPTURE_EXCEEDS_HOLD,
                    "Capture " + asset.format(captured) + " > provizyon " + asset.format(current.amount()),
                    Map.of("provisionId", provisionId, "held", asset.format(current.amount())));
        }
        Provision updated = provisions.capture(provisionId, captured).orElseThrow(() -> notHeld(provisionId));
        BigDecimal returned = updated.amount().subtract(captured);
        balances.settleHeld(updated.customerId(), updated.asset(), updated.amount(), returned);
        ledger.journal(updated.customerId(), updated.asset(), EntryType.CAPTURE, provisionId)
                .move(Bucket.HELD, Bucket.SETTLED, captured)
                .move(Bucket.HELD, Bucket.AVAILABLE, returned)
                .post();
        publish(updated, "provision.captured");
        meters.counter("provisions", "transition", "captured", "asset", updated.asset()).increment();
        return updated;
    }

    @Transactional
    public Provision release(Long actingCustomerId, UUID provisionId) {
        load(actingCustomerId, provisionId);
        Provision updated = provisions.close(provisionId, Status.RELEASED).orElseThrow(() -> notHeld(provisionId));
        returnToAvailable(updated, EntryType.RELEASE);
        publish(updated, "provision.released");
        meters.counter("provisions", "transition", "released", "asset", updated.asset()).increment();
        return updated;
    }

    /** @return süresi dolup serbest bırakılan provizyon sayısı */
    @Transactional
    public int expireDue(int batchSize) {
        List<Provision> expired = provisions.expireDue(batchSize).stream()
                // Bakiye satırlarını sabit sırayla kilitle: eşzamanlı iki süpürücü birbirini kilitlemesin (deadlock).
                .sorted(Comparator.comparingLong(Provision::customerId).thenComparing(Provision::asset))
                .toList();
        for (Provision p : expired) {
            returnToAvailable(p, EntryType.EXPIRE);
            publish(p, "provision.expired");
        }
        if (!expired.isEmpty()) {
            meters.counter("provisions", "transition", "expired", "asset", "*").increment(expired.size());
        }
        return expired.size();
    }

    public Provision get(long customerId, UUID provisionId) {
        return provisions.findById(provisionId)
                .filter(p -> p.customerId() == customerId)
                .orElseThrow(() -> notFound(provisionId));
    }

    public ProvisionView view(Provision p) {
        return ProvisionView.of(p, assets.require(p.asset()));
    }

    private Provision load(Long actingCustomerId, UUID provisionId) {
        Provision p = provisions.findById(provisionId)
                .filter(x -> actingCustomerId == null || x.customerId() == actingCustomerId)
                .orElseThrow(() -> notFound(provisionId));   // başkasının provizyonu: "yok" de, var olduğunu sızdırma
        if (actingCustomerId != null && p.operationType() == OperationType.WITHDRAWAL) {
            // Çekim provizyonunu müşteri API'den release edebilseydi: para zincire gitmişken bakiye geri gelirdi.
            throw new DomainException(ErrorCode.PROVISION_MANAGED_BY_WITHDRAWAL,
                    "Bu provizyon bir çekime ait", Map.of("provisionId", provisionId));
        }
        return p;
    }

    private void returnToAvailable(Provision p, EntryType type) {
        balances.settleHeld(p.customerId(), p.asset(), p.amount(), p.amount());
        ledger.journal(p.customerId(), p.asset(), type, p.id())
                .move(Bucket.HELD, Bucket.AVAILABLE, p.amount())
                .post();
    }

    private Instant expiryFor(OperationType type, Duration ttl) {
        if (type == OperationType.WITHDRAWAL) {
            if (ttl != null) {
                throw new DomainException(ErrorCode.INVALID_TTL, "Çekim provizyonu süresizdir");
            }
            return null;
        }
        Duration effective = ttl == null ? holdProps.defaultTtl() : ttl;
        if (effective.isNegative() || effective.isZero() || effective.compareTo(holdProps.maxTtl()) > 0) {
            throw new DomainException(ErrorCode.INVALID_TTL, "Süre 0 ile " + holdProps.maxTtl() + " arasında olmalı");
        }
        return clock.instant().plus(effective);
    }

    private DomainException notHeld(UUID provisionId) {
        Status status = provisions.findById(provisionId).map(Provision::status).orElse(null);
        return new DomainException(ErrorCode.PROVISION_NOT_HELD, "Provizyon " + status + " durumunda",
                Map.of("provisionId", provisionId, "status", String.valueOf(status)));
    }

    private static DomainException notFound(UUID provisionId) {
        return new DomainException(ErrorCode.PROVISION_NOT_FOUND, "Provizyon yok: " + provisionId,
                Map.of("provisionId", provisionId));
    }

    private void publish(Provision p, String eventType) {
        outbox.append("provision", p.id(), eventType, Map.of("customerId", p.customerId(), "provision", view(p)));
    }
}
