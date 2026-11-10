package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

/** Zorluk #3 ve #4: sıra numarasını sunucu verir (sohbet başına, boşluksuz); aynı mesaj iki kez yazılmaz, iki kez görünmez. */
class IdempotencyAndOrderingIT extends AbstractChatIT {

    @Test
    void retriedMessageIsStoredOnceAndGetsTheSameSeq() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliPhone = connect(portA, ali, "phone");
        TestClient aliTablet = connect(portB, ali, "tablet");          // aynı isteği başka gateway'den de "tekrar dener"
        TestClient aysePhone = connect(portA, ayse, "phone");

        String id = UUID.randomUUID().toString();
        Map<String, Object> send = Map.of("type", "SEND", "clientMsgId", id, "conversationId", conv, "body", "selam");
        // Tünelden çıkan telefon aynı mesajı arka arkaya üç kez gönderiyor, iki farklı gateway üzerinden
        Thread a = new Thread(() -> aliPhone.send(send));
        Thread b = new Thread(() -> aliTablet.send(send));
        a.start();
        b.start();
        a.join();
        b.join();
        aliPhone.send(send);

        JsonNode s1 = aliPhone.await("SENT", f -> f.get("clientMsgId").asText().equals(id));
        assertThat(LongStream.of(s1.get("seq").asLong())).containsOnly(1L);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> aliPhone.all("SENT").size() + aliTablet.all("SENT").size() == 3);
        List<JsonNode> acks = new ArrayList<>(aliPhone.all("SENT"));
        acks.addAll(aliTablet.all("SENT"));
        assertThat(acks).extracting(f -> f.get("seq").asLong()).containsOnly(1L);                    // hepsine aynı numara
        assertThat(acks).filteredOn(f -> !f.get("duplicate").asBoolean()).hasSize(1);                // sadece biri "yeni"

        assertThat(jdbc().sql("SELECT count(*) FROM messages WHERE conversation_id = ?").param(conv).query(Long.class).single())
                .isEqualTo(1);
        assertThat(jdbc().sql("SELECT last_seq FROM conversations WHERE id = ?").param(conv).query(Long.class).single())
                .as("tekrar denemeler sayacı boşa artırmadı (rollback)").isEqualTo(1);
        aysePhone.await("MSG", f -> f.at("/message/seq").asLong() == 1);
        Thread.sleep(1000);
        assertThat(aysePhone.receivedSeqs(conv)).containsExactly(1L);                                 // Ayşe bir kez gördü
    }

    @Test
    void concurrentSendersInAGroupGetGaplessSeqsAndEveryoneSeesTheSameOrder() throws Exception {
        long ali = user("Ali");
        long zeynep = user("Zeynep");
        long mehmet = user("Mehmet");
        long elif = user("Elif");
        long group = conversation("GROUP", Map.of(ali, "MEMBER", zeynep, "MEMBER", mehmet, "MEMBER", elif, "MEMBER"));
        TestClient aliC = connect(portA, ali, "phone");
        TestClient zeynepC = connect(portB, zeynep, "phone");
        TestClient mehmetC = connect(portA, mehmet, "phone");
        TestClient elifC = connect(portB, elif, "web");

        // Ali ve Zeynep aynı anda 30'ar mesaj atıyor, iki farklı gateway'den
        Thread t1 = new Thread(() -> { for (int i = 0; i < 30; i++) aliC.sendText(group, "ali-" + i); });
        Thread t2 = new Thread(() -> { for (int i = 0; i < 30; i++) zeynepC.sendText(group, "zeynep-" + i); });
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() ->
                mehmetC.receivedSeqs(group).size() == 60 && elifC.receivedSeqs(group).size() == 60);
        List<Long> expected = LongStream.rangeClosed(1, 60).boxed().toList();
        assertThat(mehmetC.receivedSeqs(group)).as("geliş sırası = seq sırası").containsExactlyElementsOf(expected);
        assertThat(elifC.receivedSeqs(group)).as("başka gateway'deki üye de aynı sırayı görür").containsExactlyElementsOf(expected);

        List<String> mehmetBodies = mehmetC.all("MSG").stream().map(f -> f.at("/message/body").asText()).toList();
        List<String> elifBodies = elifC.all("MSG").stream().map(f -> f.at("/message/body").asText()).toList();
        assertThat(mehmetBodies).isEqualTo(elifBodies);                                             // herkeste aynı içerik sırası
    }
}
