package io.paramix.chat.gateway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.fanout.EphemeralRelay;
import io.paramix.chat.message.ChatException;
import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.message.MessageService;
import io.paramix.chat.message.MessageService.SendCommand;
import io.paramix.chat.message.MessageService.SendResult;
import io.paramix.chat.payment.PaymentRequestService;
import io.paramix.chat.presence.PresenceService;
import io.paramix.chat.protocol.Frames;
import io.paramix.chat.protocol.Json;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Gateway'in WebSocket ucu. İş mantığı burada YOK: gelen çerçeveyi ilgili servise verir, cevabı sokete yazar.
 * Bağlanınca: yerel deftere ekle + ortak rehbere yaz. Kopunca: ikisinden de sil (rehberden sadece hâlâ bizi gösteriyorsa).
 */
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);
    static final String ATTR_USER = "userId";
    static final String ATTR_DEVICE = "deviceId";
    private static final String ATTR_SESSION = "chatSession";

    private final LocalSessions sessions;
    private final RoutingTable routing;
    private final GatewayIdentity identity;
    private final ConversationRepository conversations;
    private final MessageService messages;
    private final PaymentRequestService payments;
    private final PresenceService presence;
    private final EphemeralRelay ephemeral;
    private final ChatProperties props;
    private final Json json;
    private final MeterRegistry meters;

    public ChatWebSocketHandler(LocalSessions sessions, RoutingTable routing, GatewayIdentity identity,
                                ConversationRepository conversations, MessageService messages,
                                PaymentRequestService payments, PresenceService presence, EphemeralRelay ephemeral,
                                ChatProperties props, Json json, MeterRegistry meters) {
        this.sessions = sessions;
        this.routing = routing;
        this.identity = identity;
        this.conversations = conversations;
        this.messages = messages;
        this.payments = payments;
        this.presence = presence;
        this.ephemeral = ephemeral;
        this.props = props;
        this.json = json;
        this.meters = meters;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) throws Exception {
        long userId = (Long) raw.getAttributes().get(ATTR_USER);
        String deviceId = (String) raw.getAttributes().get(ATTR_DEVICE);
        // Soket thread-safe değil (Pub/Sub thread'i ile istek thread'i aynı anda yazabilir) → decorator.
        // Yavaş istemci: gönderim 5 sn'yi ya da 512 KB tamponu aşarsa bağlantı kapanır (backpressure), senkronla toparlar.
        WebSocketSession socket = new ConcurrentWebSocketSessionDecorator(raw,
                (int) props.gateway().sendTimeLimit().toMillis(), props.gateway().sendBufferLimit(),
                ConcurrentWebSocketSessionDecorator.OverflowStrategy.TERMINATE);
        LocalSessions.Session s = new LocalSessions.Session(userId, deviceId, socket);
        raw.getAttributes().put(ATTR_SESSION, s);
        LocalSessions.Session old = sessions.add(s, conversations.broadcastChannelsOf(userId));
        if (old != null && old.socket().isOpen()) {
            old.socket().close(CloseStatus.POLICY_VIOLATION.withReason("aynı cihaz yeniden bağlandı"));
        }
        try {
            routing.register(userId, deviceId, identity.instanceId());
        } catch (RuntimeException e) {
            // Redis yok: bağlantıyı yine kabul et. Mesaj gönderme (DB) çalışır; Redis gelince PubSubHealth rehbere yazar.
            log.warn("{}:{} rehbere yazılamadı, Redis gelince yazılacak: {}", userId, deviceId, e.getMessage());
        }
        meters.gauge("chat.gateway.connections", sessions, LocalSessions::size);
        send(socket, Frames.of("HELLO", "gateway", identity.instanceId()));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
        LocalSessions.Session s = (LocalSessions.Session) raw.getAttributes().get(ATTR_SESSION);
        if (s != null && sessions.remove(s)) {
            try {
                routing.unregister(s.userId(), s.deviceId(), identity.instanceId());
                presence.markLastSeen(s.userId());
            } catch (RuntimeException e) {
                log.debug("{} rehberden silinemedi (bayat kayıt lease ile geçersizleşir): {}", s.key(), e.getMessage());
            }
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession raw, TextMessage message) throws IOException {
        LocalSessions.Session s = (LocalSessions.Session) raw.getAttributes().get(ATTR_SESSION);
        JsonNode f;
        try {
            f = json.read(message.getPayload());
        } catch (IllegalArgumentException e) {
            send(s.socket(), Frames.error("BAD_REQUEST", "geçersiz JSON", null));
            return;
        }
        String type = f.path("type").asText();
        String ref = f.has("clientMsgId") ? f.get("clientMsgId").asText() : type;
        try {
            Map<String, Object> reply = handle(s, type, f);
            if (reply != null) {
                send(s.socket(), reply);
            }
        } catch (ChatException e) {
            send(s.socket(), Frames.error(e.code(), e.getMessage(), ref));
        } catch (RuntimeException e) {
            log.warn("{} çerçevesi işlenemedi: {}", type, e.toString());
            send(s.socket(), Frames.error("INTERNAL", "beklenmeyen hata, tekrar dene", ref));
        }
    }

    private Map<String, Object> handle(LocalSessions.Session s, String type, JsonNode f) {
        long user = s.userId();
        String device = s.deviceId();
        return switch (type) {
            case "SEND" -> {
                SendResult r = messages.send(user, device, new SendCommand(uuid(f, "clientMsgId"),
                        f.path("conversationId").asLong(), text(f, "kind"), text(f, "body"),
                        f.has("amountKurus") ? f.get("amountKurus").asLong() : null,
                        f.has("payerId") ? f.get("payerId").asLong() : null));
                yield Frames.sent(r.message().clientMsgId(), r.message().conversationId(), r.message().seq(),
                        r.message().serverTime(), r.duplicate());
            }
            case "ACK" -> {
                messages.ack(user, device, f.path("conversationId").asLong(), f.path("seq").asLong());
                yield null;
            }
            case "READ" -> {
                messages.read(user, device, f.path("conversationId").asLong(), f.path("seq").asLong());
                yield null;
            }
            case "SYNC" -> {
                Map<Long, Long> cursors = new HashMap<>();
                f.path("cursors").properties().forEach(e -> cursors.put(Long.parseLong(e.getKey()), e.getValue().asLong()));
                yield Frames.syncResult(messages.sync(user, device, cursors));
            }
            case "HEADS" -> Frames.of("HEADS_RESULT", "conversations", messages.heads(user));
            case "TYPING" -> {
                ephemeral.typing(user, device, f.path("conversationId").asLong());
                yield null;
            }
            case "PRESENCE_QUERY" -> {
                List<Long> ids = new ArrayList<>();
                f.path("userIds").forEach(n -> ids.add(n.asLong()));
                yield presence.query(ids);
            }
            case "PAY" -> {
                long id = f.path("paymentRequestId").asLong();
                yield Frames.of("PAY_RESULT", "paymentRequestId", id, "status", payments.pay(user, id));
            }
            case "DECLINE" -> {
                long id = f.path("paymentRequestId").asLong();
                yield Frames.of("DECLINE_RESULT", "paymentRequestId", id, "declined", payments.decline(user, id));
            }
            case "PING" -> Frames.of("PONG");
            default -> throw new ChatException("BAD_REQUEST", "bilinmeyen çerçeve türü: " + type);
        };
    }

    private static UUID uuid(JsonNode f, String field) {
        try {
            return f.hasNonNull(field) ? UUID.fromString(f.get(field).asText()) : null;
        } catch (IllegalArgumentException e) {
            throw new ChatException("BAD_REQUEST", field + " UUID olmalı");
        }
    }

    private static String text(JsonNode f, String field) {
        return f.hasNonNull(field) ? f.get(field).asText() : null;
    }

    private void send(WebSocketSession socket, Map<String, Object> frame) {
        try {
            socket.sendMessage(new TextMessage(json.write(frame)));
        } catch (IOException | RuntimeException e) {
            log.debug("Cevap yazılamadı: {}", e.getMessage());
        }
    }
}
