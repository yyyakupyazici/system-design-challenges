package io.paramix.provision.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.support.UuidV7;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IdempotencyUnitTest {

    private static final Instant NOW = Instant.parse("2026-11-11T23:58:00Z");

    private static ProvisionProperties props(Duration retention) {
        return new ProvisionProperties(
                new ProvisionProperties.Idempotency(Duration.ofHours(24), Duration.ofMinutes(5), retention,
                        Duration.ofSeconds(2), 3),
                null, null, null, null, null);
    }

    private final IdempotencyKeyPolicy policy =
            new IdempotencyKeyPolicy(Clock.fixed(NOW, ZoneOffset.UTC), props(Duration.ofHours(72)));

    @Test
    @DisplayName("UUIDv7: sürüm, varyant ve gömülü zaman doğru")
    void uuidV7RoundTrip() {
        UUID id = UuidV7.generate(NOW);
        assertThat(id.version()).isEqualTo(7);
        assertThat(id.variant()).isEqualTo(2);
        assertThat(UuidV7.timestampOf(id)).isEqualTo(NOW);
        assertThat(UuidV7.generate(NOW.plusMillis(1)).compareTo(id)).as("zamanla artar").isPositive();
    }

    @Test
    @DisplayName("Partition günü key'in KENDİ zamanından gelir: gece yarısını geçen tekrar aynı partition'a düşer")
    void partitionDayComesFromKeyNotFromArrivalTime() {
        UUID key = UuidV7.generate(NOW);   // 23:58'de üretildi
        IdempotencyKeyPolicy afterMidnight = new IdempotencyKeyPolicy(
                Clock.fixed(NOW.plus(Duration.ofMinutes(5)), ZoneOffset.UTC), props(Duration.ofHours(72)));

        assertThat(policy.parse(key.toString()).partitionDay()).isEqualTo(LocalDate.of(2026, 11, 11));
        assertThat(afterMidnight.parse(key.toString()).partitionDay()).isEqualTo(LocalDate.of(2026, 11, 11));
    }

    @Test
    @DisplayName("Kabul penceresi: 24 saatten eski ve 5 dakikadan ileri key'ler reddedilir")
    void acceptWindow() {
        assertThat(policy.parse(UuidV7.generate(NOW.minus(Duration.ofHours(23))).toString())).isNotNull();
        assertThatThrownBy(() -> policy.parse(UuidV7.generate(NOW.minus(Duration.ofHours(25))).toString()))
                .extracting(e -> ((IdempotencyException) e).reason())
                .isEqualTo(IdempotencyException.Reason.KEY_EXPIRED);
        assertThatThrownBy(() -> policy.parse(UuidV7.generate(NOW.plus(Duration.ofMinutes(6))).toString()))
                .extracting(e -> ((IdempotencyException) e).reason())
                .isEqualTo(IdempotencyException.Reason.KEY_EXPIRED);
    }

    @Test
    @DisplayName("Saklama süresi kabul penceresinden kısaysa uygulama başlamaz")
    void retentionShorterThanAcceptWindowFailsFast() {
        assertThatThrownBy(() -> props(Duration.ofHours(24))).isInstanceOf(IllegalStateException.class);
    }

    record Cmd(String asset, BigDecimal amount, String ref) {
    }

    @Test
    @DisplayName("Parmak izi: tutar yazımı farkı aynı, anlam farkı farklı iz verir")
    void fingerprint() {
        RequestFingerprint fp = new RequestFingerprint();
        byte[] a = fp.of("provision.create", new Cmd("TRY", new BigDecimal("100"), "o1"));
        byte[] b = fp.of("provision.create", new Cmd("TRY", new BigDecimal("100.00"), "o1"));
        byte[] c = fp.of("provision.create", new Cmd("TRY", new BigDecimal("100.01"), "o1"));
        byte[] d = fp.of("withdrawal.create", new Cmd("TRY", new BigDecimal("100"), "o1"));

        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }
}
