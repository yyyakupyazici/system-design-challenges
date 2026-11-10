package io.paramix.chat.it;

import org.awaitility.Awaitility;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Bir telefon ya da web istemcisi gibi davranan WebSocket istemcisi: gelen bütün çerçeveleri kaydeder. */
public final class TestClient implements WebSocket.Listener, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    public final long userId;
    public final String deviceId;
    private final List<JsonNode> frames = new CopyOnWriteArrayList<>();
    private final StringBuilder partial = new StringBuilder();
    private WebSocket ws;

    private TestClient(long userId, String deviceId) {
        this.userId = userId;
        this.deviceId = deviceId;
    }

    public static TestClient connect(int port, long userId, String deviceId) {
        TestClient c = new TestClient(userId, deviceId);
        c.ws = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + port + "/ws?userId=" + userId + "&deviceId=" + deviceId), c)
                .join();
        c.await("HELLO", f -> true);
        return c;
    }

    public void send(Map<String, Object> frame) {
        try {
            ws.sendText(JSON.writeValueAsString(frame), true).join();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Metin mesajı gönderir, clientMsgId döner (✓ beklemez). */
    public String sendText(long conversationId, String body) {
        String id = UUID.randomUUID().toString();
        send(Map.of("type", "SEND", "clientMsgId", id, "conversationId", conversationId, "body", body));
        return id;
    }

    public JsonNode sentAck(String clientMsgId) {
        return await("SENT", f -> f.get("clientMsgId").asText().equals(clientMsgId));
    }

    public JsonNode await(String type, Predicate<JsonNode> match) {
        return await(type, match, Duration.ofSeconds(15));
    }

    public JsonNode await(String type, Predicate<JsonNode> match, Duration timeout) {
        Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(20)).until(() -> find(type, match) != null);
        return find(type, match);
    }

    public JsonNode find(String type, Predicate<JsonNode> match) {
        return frames.stream().filter(f -> type.equals(f.path("type").asText())).filter(match).findFirst().orElse(null);
    }

    public List<JsonNode> all(String type) {
        return frames.stream().filter(f -> type.equals(f.path("type").asText())).toList();
    }

    /** Bu sohbette alınan mesajların seq'leri, GELİŞ SIRASIYLA. */
    public List<Long> receivedSeqs(long conversationId) {
        return all("MSG").stream().map(f -> f.get("message"))
                .filter(m -> m.get("conversationId").asLong() == conversationId)
                .map(m -> m.get("seq").asLong()).toList();
    }

    public boolean isOpen() {
        return !ws.isInputClosed();
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            try {
                frames.add(JSON.readTree(partial.toString()));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            partial.setLength(0);
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public void close() {
        if (!ws.isOutputClosed()) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
        }
    }
}
