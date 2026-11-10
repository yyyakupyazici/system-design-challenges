package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.paramix.chat.push.RecordingPushSender;

import com.fasterxml.jackson.databind.JsonNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Zorluk #5 ve #6: kampanya kanalı (üye listesi dolaşılmaz) ve presence (çekme modeli, gizlilik). */
class BroadcastPresenceIT extends AbstractChatIT {

    @Test
    void broadcastReachesSubscribersOnEveryGatewayWithOnePublishAndNoPush() throws Exception {
        long paramix = user("Paramix");
        long ayse = user("Ayşe");
        long mehmet = user("Mehmet");
        long offline = user("Çevrimdışı");
        long channel = conversation("BROADCAST", Map.of(paramix, "ADMIN", ayse, "MEMBER", mehmet, "MEMBER", offline, "MEMBER"));
        TestClient admin = connect(portA, paramix, "console");
        TestClient ayseC = connect(portA, ayse, "phone");
        TestClient mehmetC = connect(portB, mehmet, "phone");
        RecordingPushSender push = bean(cluster.nodeA, RecordingPushSender.class);
        long pushes = push.sent().size();

        String id = admin.sendText(channel, "Hafta sonu %20 indirim!");
        admin.sentAck(id);
        ayseC.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));
        mehmetC.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id));
        Thread.sleep(500);
        assertThat(push.sent().stream().skip(pushes)).noneMatch(p -> p.conversationId() == channel);   // 2 milyona push yok

        String bad = ayseC.sendText(channel, "ben de yazayım");                        // abone yazamaz
        assertThat(ayseC.await("ERROR", f -> f.get("ref").asText().equals(bad)).get("code").asText()).isEqualTo("FORBIDDEN");

        // Çevrimdışı abone uygulamayı açınca kanaldaki mesajı senkronla çeker (fan-out on read)
        TestClient late = connect(portB, offline, "phone");
        late.send(Map.of("type", "SYNC", "cursors", Map.of()));
        JsonNode sync = late.await("SYNC_RESULT", f -> true);
        assertThat(sync.toString()).contains("Hafta sonu %20 indirim!");
    }

    @Test
    void typingIsRelayedButNotStored() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");

        ayseC.send(Map.of("type", "TYPING", "conversationId", conv));
        ayseC.send(Map.of("type", "TYPING", "conversationId", conv));                 // 2 sn içinde ikincisi yutulur
        aliC.await("TYPING", f -> f.get("userId").asLong() == ayse);
        Awaitility.await().pollDelay(Duration.ofMillis(500)).until(() -> true);
        assertThat(aliC.all("TYPING")).hasSize(1);
        assertThat(jdbc().sql("SELECT count(*) FROM messages WHERE conversation_id = ?").param(conv).query(Long.class).single()).isZero();
    }

    @Test
    void presenceIsDerivedFromLiveConnectionsAndRespectsPrivacy() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long gizli = user("Gizli", true);
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");
        connect(portA, gizli, "phone");
        ayseC.close();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                get(portA, "/v1/presence?ids=" + ayse, ali).contains("lastSeenMs"));

        aliC.send(Map.of("type", "PRESENCE_QUERY", "userIds", List.of(ayse, gizli, ali)));
        JsonNode users = aliC.await("PRESENCE", f -> true).get("users");
        assertThat(users.get(0).get("online").asBoolean()).isFalse();
        assertThat(users.get(0).get("lastSeenMs").asLong()).isPositive();               // "son görülme"
        assertThat(users.get(1).get("hidden").asBoolean()).isTrue();                    // çevrimiçi olsa bile gizli
        assertThat(users.get(1).has("online")).isFalse();
        assertThat(users.get(2).get("online").asBoolean()).isTrue();
    }
}
