package io.paramix.redislab.presence;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * İki klasik "geçici durum" senaryosu:
 *
 * <ul>
 *   <li><b>Oturum (session) deposu:</b> kullanıcı oturumu Hash'te, her istekte süresi uzar (sliding expiration).
 *       Pod'lar durumsuz kalır, kullanıcı hangi pod'a düşerse düşsün oturumu bulunur. TTL dolunca kendiliğinden gider.</li>
 *   <li><b>Çevrimiçi kullanıcılar (presence):</b> ZSET, skor = son görülme anı. "Son 60 sn'de görülenler" = ZCOUNT.</li>
 * </ul>
 *
 * Dikkat: tek bir presence ZSET'i milyonlarca kullanıcıda sıcak key olur (bütün heartbeat'ler tek düğüme).
 * Ölçek büyüyünce kullanıcıları N parçaya bölersin ({@code presence:{0..N}}), sayıyı parçaların toplamından alırsın.
 */
@Service
public class PresenceService {

    private final StringRedisTemplate redis;

    public PresenceService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // --- oturum ---

    public void createSession(String sessionId, Map<String, String> attrs, Duration idleTimeout) {
        String key = "session:{" + sessionId + "}";
        redis.opsForHash().putAll(key, attrs);
        redis.expire(key, idleTimeout);
    }

    /** Her istekte çağrılır: oturum varsa süresini uzatır ve döner. */
    public Map<Object, Object> touchSession(String sessionId, Duration idleTimeout) {
        String key = "session:{" + sessionId + "}";
        Map<Object, Object> s = redis.opsForHash().entries(key);
        if (!s.isEmpty()) {
            redis.expire(key, idleTimeout);
        }
        return s;
    }

    // --- çevrimiçi ---

    public void heartbeat(String room, String userId) {
        redis.opsForZSet().add("presence:{" + room + "}", userId, System.currentTimeMillis());
    }

    public long onlineCount(String room, Duration window) {
        long now = System.currentTimeMillis();
        Long n = redis.opsForZSet().count("presence:{" + room + "}", now - window.toMillis(), Double.MAX_VALUE);
        return n == null ? 0 : n;
    }

    public Set<String> onlineUsers(String room, Duration window) {
        long now = System.currentTimeMillis();
        return redis.opsForZSet().rangeByScore("presence:{" + room + "}", now - window.toMillis(), Double.MAX_VALUE);
    }

    /** Eski kayıtları temizle (ZSET sonsuza kadar büyümesin). */
    public long prune(String room, Duration window) {
        Long n = redis.opsForZSet().removeRangeByScore("presence:{" + room + "}", 0, System.currentTimeMillis() - window.toMillis());
        return n == null ? 0 : n;
    }
}
