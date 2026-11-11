package io.paramix.chat.gateway;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.protocol.Frames;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;

/**
 * Hızlı yolun sağlığı, uçtan uca. Gateway her saniye KENDİ kanalına bir sonda yayınlar ve geri gelmesini bekler.
 * Sonda dönüyorsa: Redis ayakta, abonelik canlı, mesajlar bu gateway'e ulaşıyor. Bu, "Redis bağlantısı açık mı?"
 * kontrolünden daha güçlü: Redis'in yavaş aboneyi sessizce kopardığı (client-output-buffer-limit) durumu da yakalar.
 *
 * <pre>
 *  sonda probe-timeout (3 sn) boyunca gelmedi → BOZUK: bu arada gelen mesajlar bu gateway'e ulaşmamış olabilir
 *  sonda tekrar geldi                         → DÜZELDİ:
 *     ① bağlı bütün cihazları rehbere YENİDEN yaz (Redis verisini kaybederek yeniden başlamış olabilir)
 *     ② her istemciye RESYNC {afterMs: 0..resync-spread rastgele} → istemci SYNC/HEADS ile kaçırdığını çeker
 * </pre>
 */
@Component
@ConditionalOnProperty(name = "chat.roles.gateway", havingValue = "true", matchIfMissing = true)
public class PubSubHealth {

    private static final Logger log = LoggerFactory.getLogger(PubSubHealth.class);

    private final StringRedisTemplate redis;
    private final GatewayIdentity identity;
    private final LocalSessions sessions;
    private final RoutingTable routing;
    private final ChatProperties props;
    private final Json json;
    private final MeterRegistry meters;
    private final AtomicLong lastProbeSeen = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean degraded = new AtomicBoolean();

    public PubSubHealth(StringRedisTemplate redis, GatewayIdentity identity, LocalSessions sessions, RoutingTable routing,
                        ChatProperties props, Json json, MeterRegistry meters) {
        this.redis = redis;
        this.identity = identity;
        this.sessions = sessions;
        this.routing = routing;
        this.props = props;
        this.json = json;
        this.meters = meters;
        meters.gauge("chat.gateway.pubsub_degraded", degraded, d -> d.get() ? 1 : 0);
    }

    public boolean isDegraded() {
        return degraded.get();
    }

    @Scheduled(fixedDelayString = "${chat.gateway.probe-interval:1s}")
    public void probe() {
        try {
            redis.convertAndSend(GatewayIdentity.channelOf(identity.instanceId()), json.write(Map.of("probe", System.currentTimeMillis())));
        } catch (RuntimeException e) {
            log.debug("Sonda yayınlanamadı: {}", e.getMessage());
        }
        if (System.currentTimeMillis() - lastProbeSeen.get() > props.gateway().probeTimeout().toMillis()
                && degraded.compareAndSet(false, true)) {
            log.warn("Pub/Sub yolu bozuk: {} ms'dir sonda dönmedi", System.currentTimeMillis() - lastProbeSeen.get());
            meters.counter("chat.gateway.pubsub_outage").increment();
        }
    }

    /** {@link GatewayInbox} kendi sondasını gördüğünde çağırır. */
    void probeReceived() {
        lastProbeSeen.set(System.currentTimeMillis());
        if (degraded.compareAndSet(true, false)) {
            Thread.ofVirtual().start(this::recover);
        }
    }

    private void recover() {
        List<LocalSessions.Session> all = List.copyOf(sessions.all());
        int reregistered = 0;
        for (LocalSessions.Session s : all) {
            try {
                routing.register(s.userId(), s.deviceId(), identity.instanceId());
                reregistered++;
            } catch (RuntimeException e) {
                log.debug("{} rehbere yazılamadı: {}", s.key(), e.getMessage());
            }
        }
        long spread = props.gateway().resyncSpread().toMillis();
        for (LocalSessions.Session s : all) {
            long after = spread <= 0 ? 0 : ThreadLocalRandom.current().nextLong(spread);
            try {
                s.socket().sendMessage(new TextMessage(json.write(Frames.of("RESYNC", "afterMs", after))));
            } catch (IOException | RuntimeException e) {
                log.debug("{} cihazına RESYNC yazılamadı: {}", s.key(), e.getMessage());
            }
        }
        meters.counter("chat.gateway.pubsub_recovered").increment();
        log.info("Pub/Sub yolu düzeldi: {} cihaz rehbere yeniden yazıldı, {} cihaza {} ms'ye yayılmış RESYNC gönderildi",
                reregistered, all.size(), spread);
    }
}
