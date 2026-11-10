package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import io.paramix.chat.outbox.OutboxRelay;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

/** Zorluk #1: Ali'nin soketi gw-a'da, Ayşe'ninki gw-b'de. Mesaj Redis Pub/Sub üzerinden diğer makineye geçer. */
class CrossGatewayDeliveryIT extends AbstractChatIT {

    @Test
    void messageTravelsFromOneGatewayToAnotherAndTicksComeBack() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliPhone = connect(portA, ali, "phone");
        TestClient aysePhone = connect(portB, ayse, "phone");

        String id = aliPhone.sendText(conv, "selam");

        JsonNode sent = aliPhone.sentAck(id);                                       // ✓ kalıcı olarak kaydedildi
        assertThat(sent.get("seq").asLong()).isEqualTo(1);
        assertThat(sent.get("duplicate").asBoolean()).isFalse();

        JsonNode msg = aysePhone.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id)).get("message");
        assertThat(msg.get("body").asText()).isEqualTo("selam");
        assertThat(msg.get("senderId").asLong()).isEqualTo(ali);
        assertThat(msg.get("seq").asLong()).isEqualTo(1);

        aysePhone.send(Map.of("type", "ACK", "conversationId", conv, "seq", 1));     // ✓✓ iletildi
        aliPhone.await("RECEIPT", f -> f.get("receipt").asText().equals("DELIVERED") && f.get("seq").asLong() == 1);

        aysePhone.send(Map.of("type", "READ", "conversationId", conv, "seq", 1));    // mavi ✓✓ okundu
        aliPhone.await("RECEIPT", f -> f.get("receipt").asText().equals("READ") && f.get("userId").asLong() == ayse);

        assertThat(aliPhone.find("MSG", f -> true)).as("gönderen cihaz kendi mesajını tekrar almaz").isNull();
        assertThat(get(portB, "/v1/conversations/" + conv + "/messages", ayse)).contains("selam");
    }

    @Test
    void onlyOneRelayIsLeaderAtATime() {
        boolean a = bean(cluster.nodeA, OutboxRelay.class).isLeader();
        boolean b = bean(cluster.nodeB, OutboxRelay.class).isLeader();
        assertThat(a ^ b).as("advisory lock: tam olarak bir lider").isTrue();
    }

    @Test
    void outsidersAndBadFramesGetErrorsNotCrashes() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long stranger = user("Yabancı");
        long conv = direct(ali, ayse);
        TestClient s = connect(portA, stranger, "phone");

        String id = s.sendText(conv, "merhaba");
        assertThat(s.await("ERROR", f -> f.get("ref").asText().equals(id)).get("code").asText()).isEqualTo("FORBIDDEN");

        s.send(Map.of("type", "SEND", "conversationId", conv, "body", "anahtarsız"));
        assertThat(s.await("ERROR", f -> f.get("code").asText().equals("BAD_REQUEST")).get("message").asText())
                .contains("clientMsgId");
        assertThat(s.isOpen()).isTrue();
    }
}
