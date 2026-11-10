package io.paramix.chat.gateway;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.web.socket.WebSocketSession;

/**
 * Gateway'in kendi defteri: "bu kullanıcının bu cihazı bende, şu sokette." Sadece bu makinenin belleğinde.
 * Kampanya kanalları için ek bir dizin: "kanal 900'ün bende bağlı aboneleri kimler?"
 */
@org.springframework.stereotype.Component
public class LocalSessions {

    public record Session(long userId, String deviceId, WebSocketSession socket) {
        public String key() {
            return userId + ":" + deviceId;
        }
    }

    private final Map<String, Session> byDevice = new ConcurrentHashMap<>();
    private final Map<Long, Set<String>> broadcastIndex = new ConcurrentHashMap<>();

    /** Aynı cihazın eski soketi varsa döner (çağıran onu kapatır). */
    public Session add(Session s, List<Long> broadcastChannels) {
        Session old = byDevice.put(s.key(), s);
        for (Long ch : broadcastChannels) {
            broadcastIndex.computeIfAbsent(ch, k -> ConcurrentHashMap.newKeySet()).add(s.key());
        }
        return old;
    }

    /** Sadece aynı soket hâlâ kayıtlıysa siler (cihaz bu arada yeniden bağlandıysa yenisine dokunmaz). */
    public boolean remove(Session s) {
        boolean removed = byDevice.remove(s.key(), s);
        if (removed) {
            broadcastIndex.values().forEach(set -> set.remove(s.key()));
        }
        return removed;
    }

    public Session get(String deviceKey) {
        return byDevice.get(deviceKey);
    }

    public Set<String> broadcastSubscribers(long channelId) {
        return broadcastIndex.getOrDefault(channelId, Set.of());
    }

    public Collection<Session> all() {
        return byDevice.values();
    }

    public int size() {
        return byDevice.size();
    }
}
