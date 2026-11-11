package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import io.paramix.provision.TestApi.Response;
import io.paramix.provision.support.UuidV7;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@DisplayName("API idempotency")
class IdempotencyIT extends IntegrationTest {

    private static final long ALICE = 1001;
    private static final long BOB = 1002;

    @Autowired
    DataSource dataSource;

    @Test
    @DisplayName("Aynı key ile 100 eşzamanlı istek: tek provizyon, tek bakiye düşümü, 100 aynı cevap")
    void sameKeyConcurrentlyExecutesExactlyOnce() {
        fund(ALICE, "TRY", "1000");
        String key = newKey();
        String ref = newRef();

        List<Response> responses = Concurrently.run(100, i -> api.createProvision(ALICE, key, "TRY", "100", ref));

        assertThat(responses).extracting(Response::status).containsOnly(201);
        assertThat(responses).extracting(Response::body).containsOnly(responses.getFirst().body());   // bayt bayt aynı
        assertThat(responses).filteredOn(r -> !r.replayed()).hasSize(1);                             // biri yaptı
        assertThat(count("SELECT count(*) FROM provisions")).isEqualTo(1);
        assertThat(count("SELECT count(DISTINCT journal_id) FROM ledger_entries WHERE entry_type = 'HOLD'")).isEqualTo(1);
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("900");
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("100");
    }

    @Test
    @DisplayName("Aynı key farklı içerikle: 422, hiçbir şey değişmez")
    void sameKeyDifferentPayloadIsRejected() {
        fund(ALICE, "TRY", "1000");
        String key = newKey();
        String ref = newRef();
        assertThat(api.createProvision(ALICE, key, "TRY", "100", ref).status()).isEqualTo(201);

        Response tampered = api.createProvision(ALICE, key, "TRY", "900", ref);

        assertThat(tampered.status()).isEqualTo(422);
        assertThat(tampered.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("900");
        assertThat(count("SELECT count(*) FROM provisions")).isEqualTo(1);
    }

    @Test
    @DisplayName("Anlamca aynı istek (100 ↔ 100.00, alan sırası farklı) tekrar oynatılır")
    void semanticallyEqualRequestIsReplayed() {
        fund(ALICE, "TRY", "1000");
        String key = newKey();
        String ref = newRef();
        Response first = api.createProvision(ALICE, key, "TRY", "100", ref);

        Response again = api.post("/v1/provisions", ALICE, key, """
                {
                  "operationRef": "%s",
                  "operationType": "TRADE",
                  "amount": 100.00,
                  "asset": "try"
                }""".formatted(ref));

        assertThat(again.status()).isEqualTo(201);
        assertThat(again.replayed()).isTrue();
        assertThat(again.body()).isEqualTo(first.body());
    }

    @Test
    @DisplayName("Key müşteri kapsamında: iki müşteri aynı key'i kullanabilir, birbirinin cevabını göremez")
    void keyIsScopedPerCustomer() {
        fund(ALICE, "TRY", "1000");
        fund(BOB, "TRY", "1000");
        String key = newKey();

        Response alice = api.createProvision(ALICE, key, "TRY", "100", newRef());
        Response bob = api.createProvision(BOB, key, "TRY", "200", newRef());

        assertThat(alice.status()).isEqualTo(201);
        assertThat(bob.status()).isEqualTo(201);
        assertThat(bob.replayed()).isFalse();
        assertThat(bob.field("id")).isNotEqualTo(alice.field("id"));
    }

    @Test
    @DisplayName("Bir işlemin key'i başka bir işlemde kullanılamaz")
    void keyCannotBeReusedAcrossOperations() {
        fund(ALICE, "USDT", "1000");
        String key = newKey();
        assertThat(api.createProvision(ALICE, key, "USDT", "100", newRef()).status()).isEqualTo(201);

        Response withdrawal = api.withdraw(ALICE, key, "USDT", "100", "TXyz");

        assertThat(withdrawal.status()).isEqualTo(422);
        assertThat(withdrawal.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(count("SELECT count(*) FROM withdrawals")).isZero();
    }

    @Test
    @DisplayName("Geçersiz key'ler: eksik, UUIDv4, çok eski, gelecekten")
    void invalidKeysAreRejectedBeforeAnyWork() {
        fund(ALICE, "TRY", "1000");
        assertThat(api.createProvision(ALICE, null, "TRY", "1", newRef()).code()).isEqualTo("IDEMPOTENCY_KEY_MISSING");
        assertThat(api.createProvision(ALICE, UUID.randomUUID().toString(), "TRY", "1", newRef()).code())
                .isEqualTo("IDEMPOTENCY_KEY_INVALID");
        String old = UuidV7.generate(Instant.now().minus(Duration.ofHours(25))).toString();
        assertThat(api.createProvision(ALICE, old, "TRY", "1", newRef()).code()).isEqualTo("IDEMPOTENCY_KEY_EXPIRED");
        String future = UuidV7.generate(Instant.now().plus(Duration.ofHours(1))).toString();
        assertThat(api.createProvision(ALICE, future, "TRY", "1", newRef()).code()).isEqualTo("IDEMPOTENCY_KEY_EXPIRED");

        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("1000");
        assertThat(count("SELECT count(*) FROM idempotency_keys")).isZero();
    }

    @Test
    @DisplayName("İş hatası da sonuçtur: 'bakiye yetersiz' kaydedilir, aynı key ile aynen döner; yeni key yeni deneme")
    void businessErrorIsStoredAndReplayed() {
        String key = newKey();
        String ref = newRef();
        Response first = api.createProvision(ALICE, key, "BTC", "0.5", ref);
        assertThat(first.status()).isEqualTo(422);
        assertThat(first.code()).isEqualTo("INSUFFICIENT_FUNDS");

        fund(ALICE, "BTC", "1");
        Response retried = api.createProvision(ALICE, key, "BTC", "0.5", ref);
        assertThat(retried.status()).isEqualTo(422);
        assertThat(retried.replayed()).isTrue();
        assertThat(retried.body()).isEqualTo(first.body());

        // Savepoint, hatalı denemenin provizyon satırını geri aldı: aynı emir yeni key ile açılabiliyor.
        Response fresh = api.createProvision(ALICE, newKey(), "BTC", "0.5", ref);
        assertThat(fresh.status()).isEqualTo(201);
        assertThat(held(ALICE, "BTC")).isEqualByComparingTo("0.5");
    }

    @Test
    @DisplayName("Aynı key ile ilk istek sürerken ikincisi 409 + Retry-After; ilki çökerse (rollback) tekrar deneme işi yapar")
    void inFlightRequestGets409AndCrashedOwnerLeavesNoTrace() throws Exception {
        fund(ALICE, "TRY", "1000");
        String key = newKey();
        UUID uuid = UUID.fromString(key);

        try (Connection owner = dataSource.getConnection()) {
            owner.setAutoCommit(false);
            // "Sahip" istek: key'i kaptı, işini yapıyor (commit etmedi).
            try (PreparedStatement ps = owner.prepareStatement("""
                    INSERT INTO idempotency_keys (key_day, customer_id, idem_key, operation, request_hash)
                    VALUES (?, ?, ?, 'provision.create', ?)""")) {
                ps.setDate(1, Date.valueOf(UuidV7.timestampOf(uuid).atZone(ZoneOffset.UTC).toLocalDate()));
                ps.setLong(2, ALICE);
                ps.setObject(3, uuid);
                ps.setBytes(4, new byte[32]);
                ps.executeUpdate();
            }

            Response duplicate = api.createProvision(ALICE, key, "TRY", "100", newRef());
            assertThat(duplicate.status()).isEqualTo(409);
            assertThat(duplicate.code()).isEqualTo("IDEMPOTENCY_REQUEST_IN_PROGRESS");
            assertThat(duplicate.headers().firstValue("Retry-After")).contains("1");

            owner.rollback();   // sahip süreç öldü: PostgreSQL her şeyi geri aldı, key'in izi kalmadı
        }

        Response retry = api.createProvision(ALICE, key, "TRY", "100", newRef());
        assertThat(retry.status()).isEqualTo(201);
        assertThat(retry.replayed()).isFalse();
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo(new BigDecimal("900"));
    }
}
