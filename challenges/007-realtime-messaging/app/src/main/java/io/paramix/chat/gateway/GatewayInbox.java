package io.paramix.chat.gateway;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.paramix.chat.fanout.DeliveryPublisher;
import io.paramix.chat.protocol.Json;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;

/**
 * Gateway'in telsizi: açılışta kendi kanalına (gw:&lt;instance&gt;) ve kampanya kanalına (bc) abone olur.
 * Gelen her mesajda hedef cihazları KENDİ DEFTERİNDEN ({@link LocalSessions}) bulup sokete yazar.
 * Hedef artık burada değilse (az önce koptu) mesajı bırakır: veritabanında duruyor.
 *
 * <p><b>Sıra:</b> Pub/Sub mesajları geldiği sırayla tek thread'de alınır, sonra CİHAZ BAŞINA bir şeride verilir
 * (hash(cihaz) mod 16). Aynı cihazın mesajları hep aynı şeritte, sırayla yazılır; farklı cihazlar paralel işlenir.
 * Bu, outbox relay'deki "partition'a göre sıralı paralellik" fikrinin gateway içindeki hali. Mesajları gelen her
 * mesaj için ayrı thread'e vermek (Spring'in varsayılanı) aynı cihaza giden iki mesajı yarıştırır.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.gateway", havingValue = "true", matchIfMissing = true)
public class GatewayInbox implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(GatewayInbox.class);

    private static final int LANES = 16;

    private final LocalSessions sessions;
    private final PubSubHealth health;
    private final Json json;
    private final MeterRegistry meters;
    private final ExecutorService[] lanes = new ExecutorService[LANES];

    public GatewayInbox(RedisMessageListenerContainer container, GatewayIdentity identity, LocalSessions sessions,
                        PubSubHealth health, Json json, MeterRegistry meters) {
        this.sessions = sessions;
        this.health = health;
        this.json = json;
        this.meters = meters;
        for (int i = 0; i < LANES; i++) {
            lanes[i] = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("delivery-lane-" + i).factory());
        }
        container.addMessageListener(this, new ChannelTopic(GatewayIdentity.channelOf(identity.instanceId())));
        container.addMessageListener(this, new ChannelTopic(DeliveryPublisher.BROADCAST_CHANNEL));
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        JsonNode n = json.read(new String(message.getBody()));
        if (n.has("probe")) {
            health.probeReceived();                    // kendi sondamız: hızlı yol uçtan uca çalışıyor
            return;
        }
        String frame = n.get("frame").toString();
        if (n.has("channelId")) {
            for (String target : sessions.broadcastSubscribers(n.get("channelId").asLong())) {
                enqueue(target, frame);
            }
        } else {
            n.get("targets").forEach(t -> enqueue(t.asText(), frame));
        }
    }

    private void enqueue(String target, String frame) {
        lanes[Math.floorMod(target.hashCode(), LANES)].execute(() -> deliver(target, frame));
    }

    @jakarta.annotation.PreDestroy
    void shutdown() {
        for (ExecutorService lane : lanes) {
            lane.shutdown();
        }
    }

    private void deliver(String target, String frame) {
        LocalSessions.Session s = sessions.get(target);
        if (s == null || !s.socket().isOpen()) {
            meters.counter("chat.delivery.dropped", "reason", "not_here").increment();
            return;
        }
        try {
            s.socket().sendMessage(new TextMessage(frame));
            meters.counter("chat.delivery.sent").increment();
        } catch (IOException | RuntimeException e) {
            // Yavaş/kopuk istemci: decorator tampon ya da süre sınırını aşınca soketi kapatır. İstemci senkronla toparlar.
            log.debug("{} cihazına yazılamadı: {}", target, e.getMessage());
            meters.counter("chat.delivery.dropped", "reason", "write_failed").increment();
        }
    }
}
