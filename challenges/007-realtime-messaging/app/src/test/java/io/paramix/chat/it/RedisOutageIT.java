package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import io.paramix.chat.gateway.PubSubHealth;
import io.paramix.chat.gateway.RoutingTable;
import io.paramix.chat.push.RecordingPushSender;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * "DB'ye yazıldı ama Redis'e publish edilemedi" sorusunun cevabı, gerçek bir Redis kesintisiyle:
 * mesaj hiçbir durumda kaybolmaz, ve kullanıcı kaçırdığını saatler sonra değil saniyeler içinde görür.
 */
@EnabledIf("io.paramix.chat.it.RedisControl#available")
class RedisOutageIT extends AbstractChatIT {

    /** A. Kısa kesinti (failover, ağ takılması): beklemeli tekrar deneme olayı ATLAMAZ, canlı yoldan teslim edilir. */
    @Test
    void shortOutageIsBridgedByRetryWithBackoff() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");

        RedisControl.pause(4_000);
        String id = aliC.sendText(conv, "Redis donmuşken yazdım");
        assertThat(aliC.sentAck(id).get("seq").asLong()).isEqualTo(1);           // ✓ Redis'e bağlı değil: DB yolu çalışıyor

        ayseC.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id), Duration.ofSeconds(20));
        assertThat(ayseC.receivedSeqs(conv).stream().distinct()).containsExactly(1L);  // tekrar denemeler mükerrer getirebilir, istemci seq ile tekilleştirir
    }

    /**
     * B. Redis verisini kaybederek yeniden başladı (kalıcılık kapalı): rehber, gateway lease'leri, hepsi gitti.
     * ① gönderme çalışır ② çevrimdışı kullanıcıya push yine gider (push ayrı tüketicide, şüphede herkese)
     * ③ gateway sondası yolun düzeldiğini fark eder → bağlantılarını rehbere yeniden yazar → RESYNC
     * ④ istemci HEADS ile eksiğini görür, SYNC ile alır ⑤ sonraki mesajlar yine canlı gelir
     */
    @Test
    void redisRestartWithDataLossIsRecoveredByResyncAndPushStillWorks() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long mehmet = user("Mehmet");                                             // çevrimdışı
        long group = conversation("GROUP", Map.of(ali, "MEMBER", ayse, "MEMBER", mehmet, "MEMBER"));
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");
        RecordingPushSender push = bean(cluster.nodeA, RecordingPushSender.class);
        RoutingTable routing = bean(cluster.nodeA, RoutingTable.class);

        RedisControl.stop();
        String lost = aliC.sendText(group, "Redis yokken yazdım");
        assertThat(aliC.sentAck(lost).get("seq").asLong()).isEqualTo(1);         // ①

        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() ->            // ②
                push.sent().stream().anyMatch(p -> p.userId() == mehmet && p.conversationId() == group));
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> bean(cluster.nodeB, PubSubHealth.class).isDegraded());

        RedisControl.start();                                                     // BOŞ Redis
        JsonNode resync = ayseC.await("RESYNC", f -> true, Duration.ofSeconds(20));    // ③
        assertThat(resync.get("afterMs").asLong()).isBetween(0L, 1_000L);
        assertThat(routing.devices(ayse)).containsKey("phone");                  // rehber yeniden dolduruldu

        ayseC.send(Map.of("type", "HEADS"));                                     // ④
        JsonNode head = ayseC.await("HEADS_RESULT", f -> true).get("conversations");
        long lastSeq = 0;
        for (JsonNode h : head) {
            if (h.get("conversationId").asLong() == group) {
                lastSeq = h.get("lastSeq").asLong();
            }
        }
        assertThat(lastSeq).as("istemci elindekinden (0) ileride olduğunu görür").isEqualTo(1);
        ayseC.send(Map.of("type", "SYNC", "cursors", Map.of(String.valueOf(group), 0)));
        assertThat(ayseC.await("SYNC_RESULT", f -> true).toString()).contains("Redis yokken yazdım");

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !routing.devices(ali).isEmpty()); // ⑤
        String after = aliC.sendText(group, "her şey yolunda mı?");
        ayseC.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(after), Duration.ofSeconds(15));
    }
}
