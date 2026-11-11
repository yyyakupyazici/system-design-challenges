package io.paramix.provision.idempotency;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.support.UuidV7;

import org.springframework.stereotype.Component;

/**
 * Ham header'ı doğrular: UUIDv7 mi, doğum zamanı kabul penceresinde mi?
 *
 * <p>Neden pencere var: kayıtlar sonsuza dek saklanamaz (1M müşteri × günde birkaç işlem = milyonlarca satır/gün).
 * Kaydı silinmiş bir key tekrar gelirse sunucu onu "ilk kez görüyorum" sanar ve işlemi İKİNCİ kez yapar.
 * Bunu önlemenin tek dürüst yolu, kaydı silinmiş olabilecek key'leri hiç kabul etmemek. Saklama süresi
 * (retention) her zaman kabul penceresinden uzun tutulur; {@link ProvisionProperties.Idempotency} bunu açılışta
 * zorlar.
 */
@Component
public class IdempotencyKeyPolicy {

    private final Clock clock;
    private final ProvisionProperties.Idempotency props;

    public IdempotencyKeyPolicy(Clock clock, ProvisionProperties props) {
        this.clock = clock;
        this.props = props.idempotency();
    }

    public IdempotencyKey parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IdempotencyException(IdempotencyException.Reason.KEY_MISSING,
                    "Para hareketi yapan her POST bir Idempotency-Key taşımalı");
        }
        UUID uuid;
        try {
            uuid = UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw new IdempotencyException(IdempotencyException.Reason.KEY_INVALID, "UUID biçiminde değil");
        }
        if (!UuidV7.isV7(uuid)) {
            throw new IdempotencyException(IdempotencyException.Reason.KEY_INVALID,
                    "UUID sürümü " + uuid.version() + "; sürüm 7 (zaman sıralı) bekleniyor");
        }
        Instant issuedAt = UuidV7.timestampOf(uuid);
        Instant now = clock.instant();
        if (issuedAt.isBefore(now.minus(props.acceptWindow()))) {
            throw new IdempotencyException(IdempotencyException.Reason.KEY_EXPIRED,
                    "Key " + props.acceptWindow().toHours() + " saatten eski; yeni bir key ile deneyin",
                    Map.of("issuedAt", issuedAt.toString()));
        }
        if (issuedAt.isAfter(now.plus(props.maxClockSkew()))) {
            throw new IdempotencyException(IdempotencyException.Reason.KEY_EXPIRED,
                    "Key gelecekten geliyor; cihaz saatini kontrol edin",
                    Map.of("issuedAt", issuedAt.toString()));
        }
        return new IdempotencyKey(uuid, issuedAt, issuedAt.atZone(ZoneOffset.UTC).toLocalDate());
    }
}
