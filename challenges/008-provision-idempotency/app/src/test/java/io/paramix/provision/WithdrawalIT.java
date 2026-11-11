package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import io.paramix.provision.TestApi.Response;
import io.paramix.provision.withdrawal.FakeChainGateway.SimulatedCrash;
import io.paramix.provision.withdrawal.WithdrawalWorker;
import io.paramix.provision.withdrawal.WithdrawalWorker.Claim;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

@DisplayName("Çekim: lease, fencing, çökme sonrası kurtarma")
class WithdrawalIT extends IntegrationTest {

    private static final long ALICE = 3001;

    @Autowired
    WithdrawalWorker worker;

    @Autowired
    MeterRegistry meters;

    @Test
    @DisplayName("Mutlu yol: 202 → imza → yayın → onay → provizyon capture")
    void happyPath() {
        fund(ALICE, "USDT", "1000");
        Response accepted = api.withdraw(ALICE, newKey(), "USDT", "250", "TXdest");
        assertThat(accepted.status()).isEqualTo(202);
        assertThat(accepted.field("status")).isEqualTo("PENDING");
        assertThat(held(ALICE, "USDT")).isEqualByComparingTo("250");
        String id = accepted.field("id");

        assertThat(worker.runOnce()).isTrue();
        assertThat(status(id)).isEqualTo("BROADCAST");
        chain.confirm(txHash(id));
        makeDue();
        worker.runOnce();

        assertThat(status(id)).isEqualTo("CONFIRMED");
        assertThat(api.get("/v1/withdrawals/" + id, ALICE).field("status")).isEqualTo("CONFIRMED");
        assertThat(held(ALICE, "USDT")).isEqualByComparingTo("0");
        assertThat(available(ALICE, "USDT")).isEqualByComparingTo("750");
        assertThat(chain.broadcastCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("Çekim isteği de idempotent: aynı key ile 20 eşzamanlı istek → tek çekim, tek provizyon")
    void withdrawalRequestIsIdempotent() {
        fund(ALICE, "BTC", "1");
        String key = newKey();

        List<Response> responses = Concurrently.run(20, i -> api.withdraw(ALICE, key, "BTC", "0.1", "bc1qdest"));

        assertThat(responses).extracting(Response::status).containsOnly(202);
        assertThat(responses).extracting(r -> r.field("id")).containsOnly(responses.getFirst().field("id"));
        assertThat(count("SELECT count(*) FROM withdrawals")).isEqualTo(1);
        assertThat(held(ALICE, "BTC")).isEqualByComparingTo("0.1");
    }

    @Test
    @DisplayName("Süreç yayından hemen sonra ölür: kira dolunca devralan worker zincire sorar, TEKRAR GÖNDERMEZ")
    void crashAfterBroadcastDoesNotDoubleSpend() {
        fund(ALICE, "ETH", "5");
        String id = api.withdraw(ALICE, newKey(), "ETH", "2", "0xdest").field("id");
        chain.crashAfterNextBroadcast();

        Claim first = worker.claimNext().orElseThrow();
        assertThatThrownBy(() -> worker.process(first)).isInstanceOf(SimulatedCrash.class);

        // Para zincirde ama DB "SIGNED" diyor; kira hâlâ ölü worker'da.
        assertThat(status(id)).isEqualTo("SIGNED");
        assertThat(chain.distinctTransfers()).isEqualTo(1);
        assertThat(worker.claimNext()).as("kira dolmadan kimse devralamaz").isEmpty();

        expireLeases();
        worker.runOnce();                                 // devralan worker

        assertThat(status(id)).isEqualTo("BROADCAST");
        assertThat(chain.broadcastCalls()).as("ikinci yayın yapılmadı").isEqualTo(1);
        assertThat(chain.distinctTransfers()).isEqualTo(1);

        chain.confirm(txHash(id));
        makeDue();
        worker.runOnce();
        assertThat(status(id)).isEqualTo("CONFIRMED");
        assertThat(available(ALICE, "ETH")).isEqualByComparingTo("3");
        assertThat(held(ALICE, "ETH")).isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("Fencing: kirası devralınmış eski worker uyanınca hiçbir şey yazamaz")
    void staleWorkerIsFenced() {
        fund(ALICE, "USDT", "100");
        String id = api.withdraw(ALICE, newKey(), "USDT", "40", "TXdest").field("id");
        worker.runOnce();                                 // yayınlandı, onay bekleniyor
        chain.confirm(txHash(id));
        makeDue();

        Claim stale = worker.claimNext().orElseThrow();   // A kiraladı... ve 40 sn'lik GC duraklamasına girdi
        expireLeases();
        Claim fresh = worker.claimNext().orElseThrow();   // B devraldı: İKİSİ DE aynı "BROADCAST" görüntüsünde
        assertThat(fresh.withdrawal().status()).isEqualTo(stale.withdrawal().status());
        double lostBefore = meters.counter("withdrawals.lost_lease").count();

        worker.process(stale);                            // A uyandı: durum koşulu onu durdurmaz, token durdurur

        assertThat(status(id)).as("A hiçbir şey yazamadı").isEqualTo("BROADCAST");
        assertThat(held(ALICE, "USDT")).isEqualByComparingTo("40");
        assertThat(meters.counter("withdrawals.lost_lease").count()).isEqualTo(lostBefore + 1);

        worker.process(fresh);                            // gerçek sahip bitirir
        assertThat(status(id)).isEqualTo("CONFIRMED");
        assertThat(held(ALICE, "USDT")).isEqualByComparingTo("0");
        assertThat(chain.broadcastCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("Zincir işlemi kesin reddederse provizyon serbest kalır, para geri döner")
    void chainFailureReleasesFunds() {
        fund(ALICE, "BTC", "1");
        String id = api.withdraw(ALICE, newKey(), "BTC", "0.3", "bc1qdest").field("id");
        worker.runOnce();

        chain.fail(txHash(id));
        makeDue();
        worker.runOnce();

        assertThat(status(id)).isEqualTo("FAILED");
        assertThat(available(ALICE, "BTC")).isEqualByComparingTo("1");
        assertThat(held(ALICE, "BTC")).isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("Üst üste hata → MANUAL_REVIEW; para zincirde olabileceği için provizyon BLOKE kalır")
    void repeatedFailuresGoToManualReviewAndKeepFundsHeld() {
        fund(ALICE, "USDT", "100");
        String id = api.withdraw(ALICE, newKey(), "USDT", "60", "TXdest").field("id");
        worker.runOnce();                                 // imzalandı + yayınlandı
        chain.unavailableFor(100);

        for (int i = 0; i < 3; i++) {
            makeDue();
            worker.runOnce();
        }

        assertThat(status(id)).isEqualTo("MANUAL_REVIEW");
        assertThat(held(ALICE, "USDT")).isEqualByComparingTo("60");
        makeDue();
        assertThat(worker.runOnce()).as("MANUAL_REVIEW otomatik işlenmez").isFalse();
    }

    @Test
    @DisplayName("Çekim provizyonu süresiz (DB kısıtı) ve müşteri API'sinden kapatılamaz")
    void withdrawalProvisionIsOwnedByWithdrawal() {
        fund(ALICE, "TRY", "500");
        String id = api.withdraw(ALICE, newKey(), "TRY", "100", "TR000000000000000000000000").field("id");
        String provisionId = jdbc.queryForObject("SELECT provision_id::text FROM withdrawals WHERE id = ?::uuid",
                String.class, id);

        assertThat(api.release(ALICE, newKey(), provisionId).code()).isEqualTo("PROVISION_MANAGED_BY_WITHDRAWAL");
        assertThatThrownBy(() -> jdbc.update("UPDATE provisions SET expires_at = now() WHERE id = ?::uuid", provisionId))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("100");
    }

    private String status(String id) {
        return jdbc.queryForObject("SELECT status FROM withdrawals WHERE id = ?::uuid", String.class, id);
    }

    private String txHash(String id) {
        return jdbc.queryForObject("SELECT tx_hash FROM withdrawals WHERE id = ?::uuid", String.class, id);
    }

    private void makeDue() {
        jdbc.update("UPDATE withdrawals SET next_attempt_at = now() WHERE status IN ('PENDING','SIGNED','BROADCAST')");
    }

    private void expireLeases() {
        jdbc.update("UPDATE withdrawals SET lease_until = now() - interval '1 second' WHERE claim_token IS NOT NULL");
    }
}
