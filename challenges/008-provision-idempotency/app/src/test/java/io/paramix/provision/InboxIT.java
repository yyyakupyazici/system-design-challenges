package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import io.paramix.provision.support.UuidV7;
import io.paramix.provision.trade.TradeEvent;
import io.paramix.provision.trade.TradeEventHandler;
import io.paramix.provision.trade.TradeEventHandler.Outcome;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@DisplayName("Inbox: emir olaylarının tüketimi")
class InboxIT extends IntegrationTest {

    private static final long ALICE = 4001;

    @Autowired
    TradeEventHandler handler;

    @Test
    @DisplayName("Aynı olay 3 kez teslim edilir: bir kez uygulanır, ikisi DUPLICATE")
    void redeliveredEventIsAppliedOnce() {
        fund(ALICE, "TRY", "1000");
        String order = newRef();
        api.createProvision(ALICE, newKey(), "TRY", "1000", order);
        UUID eventId = UuidV7.generate(java.time.Instant.now());
        TradeEvent filled = new TradeEvent(TradeEvent.Type.ORDER_FILLED, ALICE, order, new BigDecimal("980"));

        List<Outcome> outcomes = List.of(handler.handle(eventId, filled), handler.handle(eventId, filled),
                handler.handle(eventId, filled));

        assertThat(outcomes).containsExactly(Outcome.APPLIED, Outcome.DUPLICATE, Outcome.DUPLICATE);
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("20");
        assertThat(held(ALICE, "TRY")).isEqualByComparingTo("0");
        assertThat(count("SELECT count(*) FROM inbox")).isEqualTo(1);
    }

    @Test
    @DisplayName("Rebalance: aynı olay 10 tüketiciye aynı anda düşer → tek APPLIED")
    void concurrentDeliveryOfSameEventAppliesOnce() {
        fund(ALICE, "BTC", "1");
        String order = newRef();
        api.createProvision(ALICE, newKey(), "BTC", "0.5", order);
        UUID eventId = UuidV7.generate(java.time.Instant.now());
        TradeEvent cancelled = new TradeEvent(TradeEvent.Type.ORDER_CANCELLED, ALICE, order, null);

        List<Outcome> outcomes = Concurrently.run(10, i -> handler.handle(eventId, cancelled));

        assertThat(outcomes).filteredOn(o -> o == Outcome.APPLIED).hasSize(1);
        assertThat(outcomes).filteredOn(o -> o == Outcome.DUPLICATE).hasSize(9);
        assertThat(available(ALICE, "BTC")).isEqualByComparingTo("1");
    }

    @Test
    @DisplayName("Tekrar teslim ≠ çelişki: iptalin ikinci teslimi DUPLICATE, iptal edilmiş emre YENİ dolum CONFLICT")
    void duplicateIsSilentButLateFillAfterCancelIsAConflict() {
        fund(ALICE, "USDT", "500");
        String order = newRef();
        api.createProvision(ALICE, newKey(), "USDT", "300", order);
        UUID cancelId = UuidV7.generate(java.time.Instant.now());
        TradeEvent cancel = new TradeEvent(TradeEvent.Type.ORDER_CANCELLED, ALICE, order, null);

        assertThat(handler.handle(cancelId, cancel)).isEqualTo(Outcome.APPLIED);
        assertThat(handler.handle(cancelId, cancel)).as("aynı olay, zararsız").isEqualTo(Outcome.DUPLICATE);
        Outcome lateFill = handler.handle(UuidV7.generate(java.time.Instant.now()),
                new TradeEvent(TradeEvent.Type.ORDER_FILLED, ALICE, order, new BigDecimal("300")));

        assertThat(lateFill).as("emir motoru serbest bırakılmış parayla işlem yaptı").isEqualTo(Outcome.CONFLICT);
        assertThat(count("SELECT count(*) FROM inbox WHERE outcome = 'CONFLICT'")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT detail FROM inbox WHERE outcome = 'CONFLICT'", String.class))
                .startsWith("PROVISION_NOT_HELD");
        assertThat(available(ALICE, "USDT")).as("çelişki parayı hareket ettirmedi").isEqualByComparingTo("500");
    }

    @Test
    @DisplayName("Provizyon henüz görünmüyorsa olay reddedilir ve inbox'a YAZILMAZ: tekrar denemede uygulanır")
    void eventBeforeProvisionIsRetriedNotSwallowed() {
        fund(ALICE, "TRY", "100");
        String order = newRef();
        UUID eventId = UuidV7.generate(java.time.Instant.now());
        TradeEvent cancel = new TradeEvent(TradeEvent.Type.ORDER_CANCELLED, ALICE, order, null);

        assertThatThrownBy(() -> handler.handle(eventId, cancel))
                .isInstanceOf(TradeEventHandler.ProvisionNotYetVisibleException.class);
        assertThat(count("SELECT count(*) FROM inbox")).as("rollback: iz yok").isZero();

        api.createProvision(ALICE, newKey(), "TRY", "100", order);
        assertThat(handler.handle(eventId, cancel)).isEqualTo(Outcome.APPLIED);
        assertThat(available(ALICE, "TRY")).isEqualByComparingTo("100");
    }
}
