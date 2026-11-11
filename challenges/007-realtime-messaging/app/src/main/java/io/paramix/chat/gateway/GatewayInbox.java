package io.paramix.chat.gateway;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.fanout.DeliveryPublisher;
import io.paramix.chat.protocol.Json;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
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
 *
 * <p><b>Park kutusu:</b> Pub/Sub aboneliği olmadığı anda gelen teslimatları yayıncı {@code gw:parked:<instance>}'a
 * bırakır (bkz. {@link DeliveryPublisher}). Gateway kutuyu her sonda aralığında (1 sn) atomik olarak alıp boşaltır ve
 * Pub/Sub'dan gelmiş gibi aynı şeritlerden teslim eder. Komut bağlantısı ile yapılır, yani abonelik hâlâ kopukken de
 * çalışır. Sıra: park edilen eski mesaj, arada canlı gelen yeni mesajdan SONRA ulaşabilir; istemci seq'e göre dizer.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.gateway", havingValue = "true", matchIfMissing = true)
public class GatewayInbox implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(GatewayInbox.class);

    private static final int LANES = 16;

    /** Al ve sil, araya kimse girmeden: iki kez teslim edilmesin, arada eklenen kaybolmasın. */
    private static final RedisScript<java.util.List> TAKE_ALL = new DefaultRedisScript<>("""
            local v = redis.call('LRANGE', KEYS[1], 0, -1)
            if #v > 0 then redis.call('DEL', KEYS[1]) end
            return v""", java.util.List.class);

    private final StringRedisTemplate redis;
    private final String parkedKey;
    private final int parkedMax;
    private final LocalSessions sessions;
    private final PubSubHealth health;
    private final Json json;
    private final MeterRegistry meters;
    private final ExecutorService[] lanes = new ExecutorService[LANES];

    public GatewayInbox(RedisMessageListenerContainer container, GatewayIdentity identity, LocalSessions sessions,
                        PubSubHealth health, Json json, MeterRegistry meters, StringRedisTemplate redis,
                        ChatProperties props) {
        this.redis = redis;
        this.parkedKey = GatewayIdentity.parkedKeyOf(identity.instanceId());
        this.parkedMax = props.gateway().parkedMax();
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
        handle(new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Scheduled(fixedDelayString = "${chat.gateway.probe-interval:1s}")
    public void drainParked() {
        java.util.List<?> parked;
        try {
            parked = redis.execute(TAKE_ALL, java.util.List.of(parkedKey));
        } catch (RuntimeException e) {
            log.debug("Park kutusu okunamadı (Redis yok?): {}", e.getMessage());
            return;
        }
        if (parked == null || parked.isEmpty()) {
            return;
        }
        parked.forEach(p -> handle(p.toString()));
        meters.counter("chat.delivery.unparked").increment(parked.size());
        if (parked.size() >= parkedMax) {
            health.parkedOverflow();
        }
    }

    private void handle(String payload) {
        JsonNode n = json.read(payload);
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
