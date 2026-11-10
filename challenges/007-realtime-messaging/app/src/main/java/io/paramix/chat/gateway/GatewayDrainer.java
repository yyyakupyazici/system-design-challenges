package io.paramix.chat.gateway;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.protocol.Frames;
import io.paramix.chat.protocol.Json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;

/**
 * Rolling deploy ve ölçek küçültmede geri bağlanma fırtınasını önler. Pod kapanırken (SIGTERM):
 *
 * <pre>
 *  ① readiness = REFUSING   → load balancer yeni bağlantı göndermez
 *  ② yeni el sıkışmalar reddedilir
 *  ③ her istemciye RECONNECT {afterMs: 0..drainSpread arası RASTGELE} → 50.000 istemci aynı saniyede değil,
 *     20 saniyeye yayılarak başka gateway'lere bağlanır (saniyede ~2.500)
 *  ④ soketler 1012 (service restart) ile kapatılır, yönlendirme kayıtları ve gateway lease'i silinir
 * </pre>
 *
 * Pod ÇÖKERSE bu adımlar çalışmaz; o durumda istemcinin kendi üstel geri çekilmesi (exponential backoff + jitter) ve
 * {@link AdmissionLimiter} fırtınayı düzleştirir.
 */
@Component
public class GatewayDrainer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GatewayDrainer.class);

    private final LocalSessions sessions;
    private final RoutingTable routing;
    private final LiveGateways live;
    private final GatewayIdentity identity;
    private final ChatProperties props;
    private final Json json;
    private final ApplicationContext ctx;
    private volatile boolean running;
    private volatile boolean draining;

    public GatewayDrainer(LocalSessions sessions, RoutingTable routing, LiveGateways live, GatewayIdentity identity,
                          ChatProperties props, Json json, ApplicationContext ctx) {
        this.sessions = sessions;
        this.routing = routing;
        this.live = live;
        this.identity = identity;
        this.props = props;
        this.json = json;
        this.ctx = ctx;
    }

    public boolean isDraining() {
        return draining;
    }

    /** Dönüş: RECONNECT gönderilen bağlantı sayısı. */
    public int drain() {
        draining = true;
        AvailabilityChangeEvent.publish(ctx, ReadinessState.REFUSING_TRAFFIC);
        long spread = props.gateway().drainSpread().toMillis();
        List<LocalSessions.Session> all = List.copyOf(sessions.all());
        for (LocalSessions.Session s : all) {
            long after = spread <= 0 ? 0 : ThreadLocalRandom.current().nextLong(spread);
            try {
                s.socket().sendMessage(new TextMessage(json.write(Frames.of("RECONNECT", "afterMs", after))));
                s.socket().close(CloseStatus.SERVICE_RESTARTED);
            } catch (IOException | RuntimeException e) {
                log.debug("{} kapatılırken hata: {}", s.key(), e.getMessage());
            }
            routing.unregister(s.userId(), s.deviceId(), identity.instanceId());
        }
        live.leave();
        log.info("Gateway {} boşaltıldı: {} bağlantıya {} ms'ye yayılmış RECONNECT gönderildi",
                identity.instanceId(), all.size(), spread);
        return all.size();
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        if (running) {
            drain();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** En büyük faz en önce durur: web sunucusundan ve Redis'ten önce boşalt. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
