package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Kart projeksiyonu tek başına. Test burada payment-request-service'in rolünü oynar ve "payment.events"e doğrudan
 * olay yazar: tekrar, sıra karışıklığı, yabancı sohbet gibi durumları gerçek servisle üretmek zor.
 */
class PaymentCardsIT extends AbstractChatIT {

    private static final ObjectMapper JSON = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static void publish(String type, long pr, int version, String status, long requester, long payer, long amount,
                                String originKind, Object originRef) throws Exception {
        Map<String, Object> e = new HashMap<>();
        e.put("type", type);
        e.put("paymentRequestId", pr);
        e.put("version", version);
        e.put("status", status);
        e.put("requesterId", requester);
        e.put("payerId", payer);
        e.put("amountKurus", amount);
        e.put("description", "Kira payı");
        e.put("originKind", originKind);
        e.put("originRef", String.valueOf(originRef));
        e.put("occurredAt", "2026-11-11T09:00:00Z");
        e.put("someFutureField", "yok sayılmalı");
        bean(cluster.nodeA, KafkaTemplate.class).send("payment.events", String.valueOf(pr), JSON.writeValueAsString(e)).get();
    }

    private static long count(long pr, String kind) {
        return jdbc().sql("SELECT count(*) FROM messages WHERE payment_request_id = ? AND kind = ?").params(pr, kind)
                .query(Long.class).single();
    }

    private static String cardStatus(long pr) {
        return jdbc().sql("SELECT status FROM payment_cards WHERE payment_request_id = ?").param(pr)
                .query(String.class).optional().orElse(null);
    }

    @Test
    void createdThenPaidBuildsTheCardAndOneStatusMessageEvenWhenEventsRepeat() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");
        long pr = id();

        publish("CREATED", pr, 1, "PENDING", ali, ayse, 25_000, "CHAT", conv);
        publish("CREATED", pr, 1, "PENDING", ali, ayse, 25_000, "CHAT", conv);        // en az bir kez: tekrar
        JsonNode card = ayseC.await("MSG", f -> f.at("/message/kind").asText().equals("PAYMENT_REQUEST")).get("message");
        assertThat(card.get("senderId").asLong()).as("kartı isteyen göndermiş görünür").isEqualTo(ali);
        assertThat(card.at("/paymentRequest/status").asText()).isEqualTo("PENDING");
        assertThat(card.at("/paymentRequest/amountKurus").asLong()).isEqualTo(25_000);

        publish("PAID", pr, 3, "PAID", ali, ayse, 25_000, "CHAT", conv);
        publish("PAID", pr, 3, "PAID", ali, ayse, 25_000, "CHAT", conv);
        JsonNode sys = aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM")).get("message");
        assertThat(sys.get("body").asText()).isEqualTo("✅ 250,00 TL ödendi");
        assertThat(sys.at("/paymentRequest/status").asText()).isEqualTo("PAID");

        Thread.sleep(1_000);
        assertThat(count(pr, "PAYMENT_REQUEST")).isEqualTo(1);
        assertThat(count(pr, "SYSTEM")).isEqualTo(1);
        // Kart mesajını sonradan senkronla çeken cihaz onu GÜNCEL durumla görür
        TestClient web = connect(portA, ayse, "web");
        web.send(Map.of("type", "SYNC", "cursors", Map.of(String.valueOf(conv), 0)));
        JsonNode synced = web.await("SYNC_RESULT", f -> true);
        assertThat(synced.toString()).contains("\"kind\":\"PAYMENT_REQUEST\"").doesNotContain("\"status\":\"PENDING\"");
    }

    /** Ödendi olayı oluşturuldu'dan önce geldi (ör. tüketici yeniden dengelendi, ya da eski olay geç kaldı). */
    @Test
    void outOfOrderEventsStillProduceTheRightCard() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        long pr = id();

        publish("PAID", pr, 3, "PAID", ali, ayse, 9_900, "CHAT", conv);
        publish("CREATED", pr, 1, "PENDING", ali, ayse, 9_900, "CHAT", conv);          // eski sürüm: yok sayılır
        aliC.await("MSG", f -> f.at("/message/kind").asText().equals("SYSTEM"));
        Thread.sleep(1_000);
        assertThat(cardStatus(pr)).isEqualTo("PAID");
        assertThat(count(pr, "PAYMENT_REQUEST")).isEqualTo(1);
        assertThat(count(pr, "SYSTEM")).isEqualTo(1);

        publish("REVERSED", pr, 4, "REVERSED", ali, ayse, 9_900, "CHAT", conv);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> "REVERSED".equals(cardStatus(pr)));
        assertThat(count(pr, "SYSTEM")).isEqualTo(2);
    }

    /** Kaynağı (sohbet id'si) isteyen kişi yazar. Yabancı bir sohbete kart düşüremez. */
    @Test
    void cardsAreNotWrittenIntoConversationsThePeopleAreNotIn() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long mallory = user("Mallory");
        long conv = direct(ali, ayse);
        long malloryPr = id();
        long linkPr = id();
        long badRefPr = id();

        publish("CREATED", malloryPr, 1, "PENDING", mallory, ayse, 100_000, "CHAT", conv);
        publish("CREATED", linkPr, 1, "PENDING", ali, ayse, 5_000, "LINK", "link-abc");   // sohbetle ilgisi yok
        publish("CREATED", badRefPr, 1, "PENDING", ali, ayse, 5_000, "CHAT", "not-a-number");
        long ok = id();
        publish("CREATED", ok, 1, "PENDING", ali, ayse, 1_000, "CHAT", conv);           // aynı partition değil ama işaret
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> cardStatus(ok) != null);
        Thread.sleep(500);
        assertThat(cardStatus(malloryPr)).isNull();
        assertThat(cardStatus(linkPr)).isNull();
        assertThat(cardStatus(badRefPr)).isNull();
        assertThat(jdbc().sql("SELECT count(*) FROM messages WHERE conversation_id = ?").param(conv).query(Long.class).single())
                .isEqualTo(1);
    }

    @Test
    void theSocketNoLongerAcceptsPaymentRequests() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        aliC.send(Map.of("type", "SEND", "clientMsgId", java.util.UUID.randomUUID().toString(), "conversationId", conv,
                "kind", "PAYMENT_REQUEST", "body", "100 TL"));
        assertThat(aliC.await("ERROR", f -> true).get("message").asText()).contains("payment-request-service");
    }
}
