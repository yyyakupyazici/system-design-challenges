package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.chat.payment.FakeWalletClient;
import io.paramix.chat.payment.FakeWalletClient.Mode;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Zorluk #8: mesaj servisi ile cüzdan ayrı. "Öde" iki kez ödememeli, ve sohbetteki kart ödemeyle tutarlı olmalı.
 * Ödeme yapan istemciler node A'ya bağlanır: o node'un sahte cüzdanı ve reconciler'ı aynı (gerçekte tek bir cüzdan servisi var).
 */
class PaymentRequestIT extends AbstractChatIT {

    private FakeWalletClient wallet() {
        return bean(cluster.nodeA, FakeWalletClient.class);
    }

    private long requestMoney(TestClient from, long conv, long payer, long amountKurus) {
        String id = UUID.randomUUID().toString();
        from.send(Map.of("type", "SEND", "clientMsgId", id, "conversationId", conv, "kind", "PAYMENT_REQUEST",
                "body", "Akşam yemeği", "amountKurus", amountKurus, "payerId", payer));
        from.sentAck(id);
        return from.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id), Duration.ofSeconds(1)) == null
                ? -1 : 0;
    }

    private long paymentRequestIdFrom(TestClient receiver, long conv) {
        JsonNode m = receiver.await("MSG", f -> f.at("/message/kind").asText().equals("PAYMENT_REQUEST")
                && f.at("/message/conversationId").asLong() == conv).get("message");
        assertThat(m.at("/paymentRequest/status").asText()).isEqualTo("PENDING");
        return m.at("/paymentRequest/id").asLong();
    }

    private void send(TestClient c, long conv, long payer, long amount) {
        String id = UUID.randomUUID().toString();
        c.send(Map.of("type", "SEND", "clientMsgId", id, "conversationId", conv, "kind", "PAYMENT_REQUEST",
                "body", "Akşam yemeği", "amountKurus", amount, "payerId", payer));
        c.sentAck(id);
    }

    @Test
    void pressingPayOnTwoDevicesAtOnceMovesMoneyOnceAndTheCardUpdatesEverywhere() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient aysePhone = connect(portA, ayse, "phone");
        TestClient ayseWeb = connect(portA, ayse, "web");
        send(aliC, conv, ayse, 25_000);
        long pr = paymentRequestIdFrom(aysePhone, conv);
        int before = wallet().moneyMovements();

        Thread t1 = new Thread(() -> aysePhone.send(Map.of("type", "PAY", "paymentRequestId", pr)));
        Thread t2 = new Thread(() -> ayseWeb.send(Map.of("type", "PAY", "paymentRequestId", pr)));
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        String r1 = aysePhone.await("PAY_RESULT", f -> true).get("status").asText();
        String r2 = ayseWeb.await("PAY_RESULT", f -> true).get("status").asText();
        // Kapma: sadece biri ödemeyi başlatır. Kaybeden, ilk ödeme bitmişse ALREADY_PAID, hâlâ sürüyorsa PROCESSING görür.
        assertThat(List.of(r1, r2)).containsOnlyOnce("PAID");
        assertThat(List.of(r1, r2)).filteredOn(r -> !r.equals("PAID")).allMatch(r -> r.equals("ALREADY_PAID") || r.equals("PROCESSING"));
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);

        // Sohbete sistem mesajı düştü ve kart herkeste "ödendi" oldu
        JsonNode sys = aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM")).get("message");
        assertThat(sys.get("body").asText()).contains("250,00 TL ödendi");
        assertThat(sys.at("/paymentRequest/status").asText()).isEqualTo("PAID");
        ayseWeb.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM"));

        // Tekrar basmak: yeni ödeme yok
        aysePhone.send(Map.of("type", "PAY", "paymentRequestId", pr));
        Awaitility.await().until(() -> aysePhone.all("PAY_RESULT").size() == 2);
        assertThat(aysePhone.all("PAY_RESULT").get(1).get("status").asText()).isEqualTo("ALREADY_PAID");
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);
    }

    /** En sinsi durum: para gitti ama cüzdanın cevabı kayboldu. Reconciler aynı anahtarla tekrar sorar, para ikinci kez gitmez. */
    @Test
    void lostWalletResponseIsReconciledWithTheSameIdempotencyKey() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient ayseC = connect(portA, ayse, "phone");
        send(aliC, conv, ayse, 10_000);
        long pr = paymentRequestIdFrom(ayseC, conv);
        int moves = wallet().moneyMovements();
        int calls = wallet().calls();

        wallet().script(Mode.TIMEOUT_AFTER_TRANSFER);
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr));
        assertThat(ayseC.await("PAY_RESULT", f -> true).get("status").asText()).isEqualTo("PROCESSING");
        assertThat(wallet().hasTransfer("payreq-" + pr)).as("para aslında gitti").isTrue();

        // Reconciler: ~1 sn sonra aynı anahtarla tekrar sorar → mevcut transferi alır → PAID + sistem mesajı
        JsonNode sys = aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM"), Duration.ofSeconds(15)).get("message");
        assertThat(sys.at("/paymentRequest/status").asText()).isEqualTo("PAID");
        assertThat(wallet().calls() - calls).isGreaterThanOrEqualTo(2);
        assertThat(wallet().moneyMovements() - moves).as("cüzdana iki kez soruldu, para bir kez hareket etti").isEqualTo(1);
        assertThat(jdbc().sql("SELECT count(*) FROM messages WHERE payment_request_id = ? AND kind = 'SYSTEM'").param(pr)
                .query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void rejectedPaymentCanBeRetriedAndExpiredRequestCannotBePaid() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient ayseC = connect(portA, ayse, "phone");

        send(aliC, conv, ayse, 5_000);
        long pr = paymentRequestIdFrom(ayseC, conv);
        wallet().script(Mode.INSUFFICIENT_FUNDS);
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr));
        assertThat(ayseC.await("PAY_RESULT", f -> true).get("status").asText()).isEqualTo("FAILED:INSUFFICIENT_FUNDS");
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr));                    // para yükledi, tekrar dener
        Awaitility.await().until(() -> ayseC.all("PAY_RESULT").size() == 2);
        assertThat(ayseC.all("PAY_RESULT").get(1).get("status").asText()).isEqualTo("PAID");

        // Süresi dolan istek
        send(aliC, conv, ayse, 7_000);
        long pr2 = ayseC.await("MSG", f -> f.at("/message/kind").asText().equals("PAYMENT_REQUEST")
                && f.at("/message/paymentRequest/id").asLong() != pr).at("/message/paymentRequest/id").asLong();
        jdbc().sql("UPDATE payment_requests SET expires_at = now() - interval '1 second' WHERE id = ?").param(pr2).update();
        JsonNode expired = aliC.await("MSG", f -> f.at("/message/body").asText().contains("süresi doldu"), Duration.ofSeconds(15));
        assertThat(expired.at("/message/paymentRequest/status").asText()).isEqualTo("EXPIRED");
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr2));
        Awaitility.await().until(() -> ayseC.all("PAY_RESULT").size() == 3);
        assertThat(ayseC.all("PAY_RESULT").get(2).get("status").asText()).isEqualTo("EXPIRED");
    }

    @Test
    void onlyThePayerCanPayAndRequestsNeedAValidAmount() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        send(aliC, conv, ayse, 1_000);
        long pr = jdbc().sql("SELECT id FROM payment_requests WHERE conversation_id = ?").param(conv).query(Long.class).single();

        aliC.send(Map.of("type", "PAY", "paymentRequestId", pr));                     // isteyen kendisi ödeyemez
        assertThat(aliC.await("ERROR", f -> f.get("ref").asText().equals("PAY")).get("code").asText()).isEqualTo("FORBIDDEN");

        String bad = UUID.randomUUID().toString();
        aliC.send(Map.of("type", "SEND", "clientMsgId", bad, "conversationId", conv, "kind", "PAYMENT_REQUEST",
                "body", "x", "amountKurus", -5, "payerId", ayse));
        assertThat(aliC.await("ERROR", f -> f.get("ref").asText().equals(bad)).get("code").asText()).isEqualTo("BAD_REQUEST");
    }
}
