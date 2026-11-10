package io.paramix.chat.presence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.paramix.chat.gateway.LiveGateways;
import io.paramix.chat.gateway.RoutingTable;
import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.protocol.Frames;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Presence ayrı bir "heartbeat" sistemi değil, yönlendirme tablosundan TÜRETİLİR:
 * çevrimiçi = canlı bir gateway'de en az bir cihazı var. Ayrı heartbeat olsaydı 1,2 milyon kullanıcı × 30 sn
 * = 40.000 yazma/sn; burada ek yazma sadece "son görülme" (son cihaz koptuğunda bir kez).
 *
 * <p>Yayma modeli ÇEKME (pull): istemci ekrandaki kişiler için sorar (en fazla 100). Değişiklikleri herkese
 * İTMEK (push) ünlü hesap problemini doğurur: 100.000 takipçili biri her bağlanıp koptuğunda 100.000 bildirim.
 */
@Service
public class PresenceService {

    public static final int MAX_QUERY = 100;

    private final RoutingTable routing;
    private final LiveGateways live;
    private final StringRedisTemplate redis;
    private final ConversationRepository conversations;

    public PresenceService(RoutingTable routing, LiveGateways live, StringRedisTemplate redis,
                           ConversationRepository conversations) {
        this.routing = routing;
        this.live = live;
        this.redis = redis;
        this.conversations = conversations;
    }

    public boolean online(long userId) {
        return routing.devices(userId).values().stream().anyMatch(live::isAlive);
    }

    /** Son cihaz koptuğunda çağrılır. */
    public void markLastSeen(long userId) {
        if (!online(userId)) {
            redis.opsForValue().set("lastseen:{" + userId + "}", String.valueOf(System.currentTimeMillis()));
        }
    }

    public Map<String, Object> query(List<Long> userIds) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Long u : userIds.stream().limit(MAX_QUERY).toList()) {
            if (conversations.presenceHidden(u)) {
                out.add(Frames.of("USER", "userId", u, "hidden", true));      // gizlilik: ne çevrimiçi ne son görülme
                continue;
            }
            boolean on = online(u);
            String last = on ? null : redis.opsForValue().get("lastseen:{" + u + "}");
            out.add(Frames.of("USER", "userId", u, "online", on, "lastSeenMs", last == null ? null : Long.parseLong(last)));
        }
        return Frames.of("PRESENCE", "users", out);
    }
}
