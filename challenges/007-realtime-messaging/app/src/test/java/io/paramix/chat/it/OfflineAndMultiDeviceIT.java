package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import io.paramix.chat.gateway.RoutingTable;
import io.paramix.chat.push.RecordingPushSender;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Zorluk #7: çevrimdışı dönüş ve çoklu cihaz. Kaçırılan mesajın tek kaynağı veritabanı; teslim yolu kaybederse senkron yakalar. */
class OfflineAndMultiDeviceIT extends AbstractChatIT {

    private RecordingPushSender push() {
        return bean(cluster.nodeA, RecordingPushSender.class);
    }

    @Test
    void offlineUserGetsPushThenSyncsMissedMessagesFromTheCursor() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        for (int i = 1; i <= 3; i++) {
            aliC.sentAck(aliC.sendText(conv, "mesaj " + i));
        }
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                push().sent().stream().filter(p -> p.userId() == ayse).count() == 3);
        assertThat(push().sent()).filteredOn(p -> p.userId() == ayse).allMatch(p -> p.preview().equals("Yeni mesaj"));
        assertThat(push().sent()).noneMatch(p -> p.userId() == ali);                     // gönderene push gitmez

        // Ayşe yeni bir cihazdan açıyor: imleci yok → son mesajlar gelir
        TestClient ayseC = connect(portB, ayse, "phone");
        ayseC.send(Map.of("type", "SYNC", "cursors", Map.of()));
        JsonNode sync = ayseC.await("SYNC_RESULT", f -> true);
        JsonNode c = conversationIn(sync, conv);
        assertThat(c.get("messages")).extracting(m -> m.get("seq").asLong()).containsExactly(1L, 2L, 3L);
        ayseC.send(Map.of("type", "ACK", "conversationId", conv, "seq", 3));
        aliC.await("RECEIPT", f -> f.get("receipt").asText().equals("DELIVERED") && f.get("seq").asLong() == 3);
        ayseC.close();

        aliC.sentAck(aliC.sendText(conv, "mesaj 4"));
        aliC.sentAck(aliC.sendText(conv, "mesaj 5"));

        // Aynı cihaz geri geliyor, imleç göndermeden: sunucudaki cihaz imleci (3) kullanılır → sadece 4 ve 5
        TestClient ayseAgain = connect(portA, ayse, "phone");
        ayseAgain.send(Map.of("type", "SYNC", "cursors", Map.of()));
        JsonNode again = conversationIn(ayseAgain.await("SYNC_RESULT", f -> true), conv);
        assertThat(again.get("messages")).extracting(m -> m.get("seq").asLong()).containsExactly(4L, 5L);
        assertThat(again.get("hasMore").asBoolean()).isFalse();

        // İstemci kendi imlecini gönderirse o kazanır ("41'den sonrasını ver")
        ayseAgain.send(Map.of("type", "SYNC", "cursors", Map.of(String.valueOf(conv), 4)));
        Awaitility.await().until(() -> ayseAgain.all("SYNC_RESULT").size() == 2);
        JsonNode third = conversationIn(ayseAgain.all("SYNC_RESULT").get(1), conv);
        assertThat(third.get("messages")).extracting(m -> m.get("seq").asLong()).containsExactly(5L);
    }

    @Test
    void deviceOnADeadGatewayCountsAsOfflineAndStaleRouteIsIgnored() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        // Ayşe'nin web'i çöken bir gateway'deydi: rehberde bayat kayıt kaldı, kimse silmedi
        bean(cluster.nodeA, RoutingTable.class).register(ayse, "web", "gw-dead#000000");
        TestClient aliC = connect(portA, ali, "phone");

        aliC.sentAck(aliC.sendText(conv, "orada mısın?"));
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                push().sent().stream().anyMatch(p -> p.userId() == ayse && p.conversationId() == conv));

        // Telefonu canlı bir gateway'e bağlanınca canlı teslim olur, push gitmez
        TestClient aysePhone = connect(portB, ayse, "phone");
        long pushesBefore = push().sent().stream().filter(p -> p.userId() == ayse).count();
        String id = aliC.sendText(conv, "şimdi gördün mü?");
        aysePhone.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));
        assertThat(push().sent().stream().filter(p -> p.userId() == ayse).count()).isEqualTo(pushesBefore);
    }

    @Test
    void allDevicesGetTheMessageAndReadingOnOneDeviceMarksItReadOnTheOthers() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliPhone = connect(portA, ali, "phone");
        TestClient aliWeb = connect(portB, ali, "web");
        TestClient aysePhone = connect(portA, ayse, "phone");
        TestClient ayseWeb = connect(portB, ayse, "web");

        String id = aliPhone.sendText(conv, "akşam?");
        aysePhone.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));
        ayseWeb.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));
        aliWeb.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));          // gönderenin diğer cihazı da görür

        aysePhone.send(Map.of("type", "READ", "conversationId", conv, "seq", 1));
        ayseWeb.await("RECEIPT", f -> f.get("receipt").asText().equals("READ") && f.get("userId").asLong() == ayse);
        aliPhone.await("RECEIPT", f -> f.get("receipt").asText().equals("READ"));
        aliWeb.await("RECEIPT", f -> f.get("receipt").asText().equals("READ"));

        // İkinci kez okumak (ya da geriye doğru okumak) yeni olay üretmez
        aysePhone.send(Map.of("type", "READ", "conversationId", conv, "seq", 1));
        aysePhone.send(Map.of("type", "PING"));
        aysePhone.await("PONG", f -> true);
        assertThat(aliPhone.all("RECEIPT").stream().filter(f -> f.get("receipt").asText().equals("READ"))).hasSize(1);
    }

    private static JsonNode conversationIn(JsonNode syncResult, long conv) {
        for (JsonNode c : syncResult.get("conversations")) {
            if (c.get("conversationId").asLong() == conv) {
                return c;
            }
        }
        throw new AssertionError("senkronda sohbet " + conv + " yok: " + syncResult);
    }
}
