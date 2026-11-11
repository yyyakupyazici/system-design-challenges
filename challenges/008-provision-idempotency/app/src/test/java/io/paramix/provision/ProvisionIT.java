package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import io.paramix.provision.TestApi.Response;
import io.paramix.provision.domain.ProvisionExpirySweeper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

@DisplayName("Provizyon durum makinesi ve bakiye")
class ProvisionIT extends IntegrationTest {

    private static final long ALICE = 2001;
    private static final long MALLORY = 2666;

    @Autowired
    ProvisionExpirySweeper sweeper;

    @Test
    @DisplayName("50 eşzamanlı provizyon, 1000 TRY bakiye, her biri 30 TRY: tam 33'ü geçer, bakiye asla eksiye düşmez")
    void concurrentHoldsNeverOverdraw() {
        fund(ALICE, "TRY", "1000");

        List<Response> responses = Concurrently.run(50, i -> api.createProvision(ALICE, newKey(), "TRY", "30", newRef()));

        assertThat(responses).filteredOn(r -> r.status() == 201).hasSize(33);
        assertThat(responses).filteredOn(r -> r.status() == 422)
                .hasSize(17)
                .allSatisfy(r -> assertThat(r.code()).isEqualTo("INSUFFICIENT_FUNDS"));
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("10");
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("990");
    }

    @Test
    @DisplayName("İş seviyesi idempotency: aynı emir için FARKLI key ile ikinci provizyon açılamaz")
    void sameOperationRefWithDifferentKeyConflicts() {
        fund(ALICE, "ETH", "10");
        String ref = newRef();
        Response first = api.createProvision(ALICE, newKey(), "ETH", "1.5", ref);

        Response second = api.createProvision(ALICE, newKey(), "ETH", "1.5", ref);

        assertThat(second.status()).isEqualTo(409);
        assertThat(second.code()).isEqualTo("PROVISION_ALREADY_EXISTS");
        assertThat(second.json().at("/context/provisionId").asText()).isEqualTo(first.field("id"));
        assertThat(held(ALICE, "ETH")).isEqualByComparingTo("1.5");
    }

    @Test
    @DisplayName("Kısmi capture: 1000 bloke, 980'e doldu → 20 geri döner; ikinci capture 409, aynı key tekrar oynatılır")
    void partialCaptureReturnsRemainder() {
        fund(ALICE, "TRY", "1000");
        String id = api.createProvision(ALICE, newKey(), "TRY", "1000", newRef()).field("id");
        String captureKey = newKey();

        Response captured = api.capture(ALICE, captureKey, id, "980");

        assertThat(captured.status()).isEqualTo(200);
        assertThat(captured.field("status")).isEqualTo("CAPTURED");
        assertThat(captured.field("capturedAmount")).isEqualTo("980.00");
        assertThat(captured.field("releasedAmount")).isEqualTo("20.00");
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("20");
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("0");

        assertThat(api.capture(ALICE, newKey(), id, "980").code()).isEqualTo("PROVISION_NOT_HELD");
        Response replay = api.capture(ALICE, captureKey, id, "980");
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.body()).isEqualTo(captured.body());
    }

    @Test
    @DisplayName("Aynı provizyona eşzamanlı 10 capture + 10 release: tam biri kazanır")
    void concurrentCaptureAndReleaseExactlyOneWins() {
        fund(ALICE, "BTC", "1");
        String id = api.createProvision(ALICE, newKey(), "BTC", "0.4", newRef()).field("id");

        List<Response> responses = Concurrently.run(20, i -> i % 2 == 0
                ? api.capture(ALICE, newKey(), id, null)
                : api.release(ALICE, newKey(), id));

        assertThat(responses).filteredOn(r -> r.status() == 200).hasSize(1);
        assertThat(responses).filteredOn(r -> r.status() == 409)
                .hasSize(19)
                .allSatisfy(r -> assertThat(r.code()).isEqualTo("PROVISION_NOT_HELD"));
        assertThat(held(ALICE, "BTC")).isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("Capture tutarı provizyondan büyük olamaz")
    void captureCannotExceedHold() {
        fund(ALICE, "TRY", "100");
        String id = api.createProvision(ALICE, newKey(), "TRY", "50", newRef()).field("id");

        assertThat(api.capture(ALICE, newKey(), id, "50.01").code()).isEqualTo("CAPTURE_EXCEEDS_HOLD");
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("50");
    }

    @Test
    @DisplayName("Süresi dolan provizyon serbest kalır; capture artık mümkün değil")
    void expiredProvisionIsReleased() {
        fund(ALICE, "USDT", "500");
        String id = api.post("/v1/provisions", ALICE, newKey(), """
                {"asset":"USDT","amount":"200","operationType":"TRADE","operationRef":"%s","ttlSeconds":60}"""
                .formatted(newRef())).field("id");
        jdbc.update("UPDATE provisions SET expires_at = now() - interval '1 second' WHERE id = ?::uuid", id);

        assertThat(sweeper.sweep()).isEqualTo(1);

        assertThat(api.get("/v1/provisions/" + id, ALICE).field("status")).isEqualTo("EXPIRED");
        assertThat(available(ALICE, "USDT")).isEqualByComparingTo("500");
        assertThat(api.capture(ALICE, newKey(), id, null).code()).isEqualTo("PROVISION_NOT_HELD");
    }

    @Test
    @DisplayName("Başkasının provizyonu: 404 (var olduğu bile sızmaz)")
    void cannotTouchAnotherCustomersProvision() {
        fund(ALICE, "TRY", "100");
        String id = api.createProvision(ALICE, newKey(), "TRY", "100", newRef()).field("id");

        assertThat(api.release(MALLORY, newKey(), id).code()).isEqualTo("PROVISION_NOT_FOUND");
        assertThat(api.get("/v1/provisions/" + id, MALLORY).status()).isEqualTo(404);
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("100");
    }

    @Test
    @DisplayName("Tutar varlığın ölçeğinden hassas olamaz (yuvarlama yok): 10.001 TRY reddedilir")
    void amountMorePreciseThanAssetIsRejected() {
        fund(ALICE, "TRY", "100");

        Response r = api.createProvision(ALICE, newKey(), "TRY", "10.001", newRef());

        assertThat(r.status()).isEqualTo(422);
        assertThat(r.code()).isEqualTo("INVALID_AMOUNT");
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("100");
    }

    @Test
    @DisplayName("Yatırım doğal anahtarla tekilleşir: aynı referans iki kez → bakiye bir kez; farklı tutar → 409")
    void depositIsDeduplicatedByExternalReference() {
        Response first = api.deposit(ALICE, "BTC", "0.25", "CHAIN", "btc:abc123:0");
        Response again = api.deposit(ALICE, "BTC", "0.25000000", "CHAIN", "btc:abc123:0");
        Response conflicting = api.deposit(ALICE, "BTC", "2.5", "CHAIN", "btc:abc123:0");

        assertThat(first.status()).isEqualTo(201);
        assertThat(again.status()).isEqualTo(200);
        assertThat(again.field("id")).isEqualTo(first.field("id"));
        assertThat(conflicting.status()).isEqualTo(409);
        assertThat(conflicting.code()).isEqualTo("DEPOSIT_REF_CONFLICT");
        assertThat(available(ALICE, "BTC")).isEqualByComparingTo("0.25");
    }

    @Test
    @DisplayName("Ledger append-only: geçmiş kayıt değiştirilemez, silinemez")
    void ledgerIsAppendOnly() {
        fund(ALICE, "TRY", "100");

        assertThatThrownBy(() -> jdbc.update("UPDATE ledger_entries SET delta = delta * 2"))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("DELETE FROM ledger_entries"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
