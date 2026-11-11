package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import io.paramix.chat.payment.FakeWalletClient;
import io.paramix.chat.payment.FakeWalletClient.Mode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Cüzdan → sohbet yönü. Test burada 006'daki cüzdanın rolünü oynar: "wallet.events" topic'ine olay yayınlar.
 * Hızlı yol (senkron cevap), olay yolu (bu test) ve güvenlik ağı (reconciler) aynı koşullu geçişlerden geçer:
 * hangisi önce gelirse o yazar, sohbete aynı mesaj iki kez düşmez.
 */
class WalletEventsIT extends AbstractChatIT {

    private static final ObjectMapper JSON = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static void walletPublishes(String type, String transferId, String key, long amount, String reason) throws Exception {
        KafkaTemplate<String, String> kafka = bean(cluster.nodeA, KafkaTemplate.class);
        Map<String, Object> e = new java.util.HashMap<>(Map.of("type", type, "transferId", transferId, "amountKurus", amount,
                "fromUserId", 1, "toUserId", 2));
        if (key != null) {
            e.put("idempotencyKey", key);
        }
        if (reason != null) {
            e.put("reason", reason);
        }
        kafka.send("wallet.events", transferId, JSON.writeValueAsString(e)).get();
    }

    private static long request(TestClient from, long conv, long payer, long amount) {
        String id = UUID.randomUUID().toString();
        from.send(Map.of("type", "SEND", "clientMsgId", id, "conversationId", conv, "kind", "PAYMENT_REQUEST",
                "body", "Kira payı", "amountKurus", amount, "payerId", payer));
        from.sentAck(id);
        return jdbc().sql("SELECT id FROM payment_requests WHERE conversation_id = ? ORDER BY id DESC LIMIT 1")
                .param(conv).query(Long.class).single();
    }

    private static String status(long pr) {
        return jdbc().sql("SELECT status FROM payment_requests WHERE id = ?").param(pr).query(String.class).single();
    }

    private static long systemMessages(long pr) {
        return jdbc().sql("SELECT count(*) FROM messages WHERE payment_request_id = ? AND kind = 'SYSTEM'")
                .param(pr).query(Long.class).single();
    }

    /** Ayşe sohbetteki "Öde"ye basmadı, aynı isteği cüzdan ekranından ödedi. Sohbetteki kart yine de "ödendi" olur. */
    @Test
    void paymentMadeOutsideTheChatIsReflectedInTheChatOnce() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        long pr = request(aliC, conv, ayse, 120_000);

        walletPublishes("TRANSFER_COMPLETED", "trf_outside_" + pr, "payreq-" + pr, 120_000, null);
        JsonNode sys = aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM")).get("message");
        assertThat(sys.at("/paymentRequest/status").asText()).isEqualTo("PAID");
        assertThat(sys.get("body").asText()).contains("1.200,00 TL ödendi");

        walletPublishes("TRANSFER_COMPLETED", "trf_outside_" + pr, "payreq-" + pr, 120_000, null);   // en az bir kez: tekrar gelir
        Thread.sleep(1_000);
        assertThat(systemMessages(pr)).isEqualTo(1);
        assertThat(status(pr)).isEqualTo("PAID");
    }

    /** Ödendikten günler sonra itiraz (chargeback): sohbetteki kart "iade edildi" olur. */
    @Test
    void laterReversalIsReflectedInTheChat() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient ayseC = connect(portA, ayse, "phone");
        long pr = request(aliC, conv, ayse, 9_900);
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr));
        assertThat(ayseC.await("PAY_RESULT", f -> true).get("status").asText()).isEqualTo("PAID");
        String transferId = jdbc().sql("SELECT wallet_transfer_id FROM payment_requests WHERE id = ?").param(pr)
                .query(String.class).single();

        walletPublishes("TRANSFER_REVERSED", transferId, null, 9_900, "CHARGEBACK");
        JsonNode rev = aliC.await("MSG", f -> f.at("/message/body").asText().contains("iade edildi")).get("message");
        assertThat(rev.at("/paymentRequest/status").asText()).isEqualTo("REVERSED");
        ayseC.await("MSG", f -> f.at("/message/body").asText().contains("iade edildi"));

        walletPublishes("TRANSFER_REVERSED", transferId, null, 9_900, "CHARGEBACK");
        Thread.sleep(1_000);
        assertThat(systemMessages(pr)).as("bir 'ödendi' + bir 'iade edildi'").isEqualTo(2);
        assertThat(jdbc().sql("SELECT reversal_reason FROM payment_requests WHERE id = ?").param(pr).query(String.class).single())
                .isEqualTo("CHARGEBACK");
    }

    /** Cüzdan yavaş cevap veriyor ve olayı cevaptan ÖNCE yayınlıyor: olay yolu yazar, senkron yol mesajı tekrarlamaz. */
    @Test
    void eventArrivingBeforeTheSynchronousResponseDoesNotDuplicateTheMessage() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient ayseC = connect(portA, ayse, "phone");
        FakeWalletClient wallet = bean(cluster.nodeA, FakeWalletClient.class);
        long pr = request(aliC, conv, ayse, 4_500);
        int moves = wallet.moneyMovements();

        wallet.script(Mode.SLOW_OK);                                            // cevap 1,5 sn gecikecek
        ayseC.send(Map.of("type", "PAY", "paymentRequestId", pr));
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> "PROCESSING".equals(status(pr)));
        walletPublishes("TRANSFER_COMPLETED", "trf_event_" + pr, "payreq-" + pr, 4_500, null);

        aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM"));
        assertThat(ayseC.await("PAY_RESULT", f -> true, Duration.ofSeconds(10)).get("status").asText()).isEqualTo("PAID");
        Thread.sleep(500);
        assertThat(systemMessages(pr)).isEqualTo(1);
        assertThat(jdbc().sql("SELECT wallet_transfer_id FROM payment_requests WHERE id = ?").param(pr).query(String.class).single())
                .as("durumu olay yolu yazdı").isEqualTo("trf_event_" + pr);
        assertThat(wallet.moneyMovements() - moves).isEqualTo(1);
    }

    /** Tutarı uyuşmayan ya da para isteğinden gelmeyen olay sohbeti değiştirmez. */
    @Test
    void mismatchedOrUnrelatedEventsAreIgnored() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        long pr = request(aliC, conv, ayse, 50_000);

        walletPublishes("TRANSFER_COMPLETED", "trf_wrong_" + pr, "payreq-" + pr, 5_000, null);     // 500 TL istendi, 50 TL geldi
        walletPublishes("TRANSFER_COMPLETED", "trf_other_" + pr, "qr-payment-77", 50_000, null);   // başka bir ödeme
        walletPublishes("TRANSFER_REVERSED", "trf_unknown_" + pr, null, 50_000, "FRAUD");          // bizim olmayan transfer
        Thread.sleep(1_500);
        assertThat(status(pr)).isEqualTo("PENDING");
        assertThat(systemMessages(pr)).isZero();
    }
}
