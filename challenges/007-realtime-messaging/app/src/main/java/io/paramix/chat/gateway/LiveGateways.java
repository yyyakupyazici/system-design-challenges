package io.paramix.chat.gateway;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.paramix.chat.config.ChatProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Gateway lease'i. Kullanıcı kayıtları süresiz; süre sadece GATEWAY'lerde:
 *
 * <pre>
 *  ZSET gw:alive   üye = gateway instance id, skor = son "yaşıyorum" anı (ms)
 *  her gateway 5 sn'de bir kendi skorunu yeniler        → 25 gateway için 5 yazma/sn
 *  (kullanıcı başına yenileme olsaydı 1,2 milyon bağlantı / 30 sn = 40.000 yazma/sn)
 *  skoru 15 sn'den eski olan gateway ÖLÜ sayılır        → onun bütün bağlantı kayıtları birden geçersiz
 * </pre>
 *
 * Canlı gateway listesi her saniye bir kez okunup bellekte tutulur: fan-out her mesajda Redis'e "gw-7 canlı mı?"
 * diye sormaz.
 */
@Component
public class LiveGateways {

    private static final Logger log = LoggerFactory.getLogger(LiveGateways.class);
    static final String KEY = "gw:alive";

    private final StringRedisTemplate redis;
    private final ChatProperties props;
    private final GatewayIdentity identity;
    private final boolean isGateway;
    private final AtomicReference<Set<String>> alive = new AtomicReference<>(Set.of());

    public LiveGateways(StringRedisTemplate redis, ChatProperties props, GatewayIdentity identity,
                        @Value("${chat.roles.gateway:true}") boolean isGateway) {
        this.redis = redis;
        this.props = props;
        this.identity = identity;
        this.isGateway = isGateway;
    }

    @Scheduled(fixedDelayString = "${chat.gateway.heartbeat:5s}")
    public void heartbeat() {
        if (isGateway) {
            try {
                redis.opsForZSet().add(KEY, identity.instanceId(), System.currentTimeMillis());
            } catch (RuntimeException e) {
                log.debug("Gateway lease yenilenemedi (Redis yok?): {}", e.getMessage());
            }
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void refresh() {
        try {
            long now = System.currentTimeMillis();
            long cutoff = now - props.gateway().lease().toMillis();
            Set<String> s = redis.opsForZSet().rangeByScore(KEY, cutoff, Double.MAX_VALUE);
            alive.set(s == null ? Set.of() : Set.copyOf(s));
            redis.opsForZSet().removeRangeByScore(KEY, 0, now - 10 * props.gateway().lease().toMillis());   // çok eskileri temizle
        } catch (RuntimeException e) {
            log.warn("Canlı gateway listesi yenilenemedi, eski liste kullanılıyor: {}", e.getMessage());
        }
    }

    public boolean isAlive(String instanceId) {
        return alive.get().contains(instanceId);
    }

    public void leave() {
        redis.opsForZSet().remove(KEY, identity.instanceId());
    }
}
