package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import io.paramix.chat.gateway.GatewayDrainer;
import io.paramix.chat.gateway.RoutingTable;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Zorluk #2: rolling deploy. Üçüncü bir gateway (gw-c) açılır, bağlantılar alır, sonra boşaltılır:
 * istemciler RASTGELE bekleme süreleriyle RECONNECT alır, kayıtlar silinir, yeni bağlantı kabul edilmez.
 */
class GatewayDrainIT extends AbstractChatIT {

    @Test
    void drainingSpreadsReconnectsOverTimeAndStopsAcceptingConnections() {
        ConfigurableApplicationContext nodeC = cluster.start("gw-c", false);
        int portC = ChatCluster.port(nodeC);
        try {
            List<TestClient> clients = new ArrayList<>();
            List<Long> users = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                long u = user("u" + i);
                users.add(u);
                clients.add(connect(portC, u, "phone"));
            }
            RoutingTable routing = bean(cluster.nodeA, RoutingTable.class);
            assertThat(routing.devices(users.getFirst())).containsKey("phone");

            int drained = bean(nodeC, GatewayDrainer.class).drain();
            assertThat(drained).isEqualTo(30);

            List<Long> waits = clients.stream()
                    .map(c -> c.await("RECONNECT", f -> true).get("afterMs").asLong()).toList();
            assertThat(waits).allMatch(w -> w >= 0 && w < 2000);                         // drain-spread = 2 sn
            assertThat(waits.stream().distinct().count()).as("hepsi aynı anda değil, zamana yayılmış").isGreaterThan(20);
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> clients.stream().noneMatch(TestClient::isOpen));
            assertThat(routing.devices(users.getFirst())).doesNotContainKey("phone");      // rehberden silindi

            // Boşaltılan gateway yeni bağlantı kabul etmez (503)
            CompletionException rejected = org.junit.jupiter.api.Assertions.assertThrows(CompletionException.class, () ->
                    HttpClient.newHttpClient().newWebSocketBuilder()
                            .buildAsync(URI.create("ws://localhost:" + portC + "/ws?userId=1&deviceId=x"), new WebSocket.Listener() { })
                            .join());
            assertThat(((java.net.http.WebSocketHandshakeException) rejected.getCause()).getResponse().statusCode()).isEqualTo(503);

            // İstemci yeniden bağlanır, başka gateway'den devam eder
            TestClient back = connect(portA, users.getFirst(), "phone");
            assertThat(back.isOpen()).isTrue();
        } finally {
            nodeC.close();
        }
    }
}
