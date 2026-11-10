package io.paramix.chat.gateway;

import java.util.List;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Ortak rehber: "bu kullanıcının bu cihazı şu an hangi gateway'de?"
 *
 * <pre>
 *  HASH conn:{userId}   alan = cihaz id, değer = gateway instance id
 *  bağlandı  → HSET
 *  koptu     → HDEL, AMA sadece kayıt hâlâ BENİ gösteriyorsa (cihaz bu arada başka gateway'e bağlanmış olabilir)
 *  gateway öldü → kimse silmez; okuyan taraf gateway'in canlı olmadığını görür ve kaydı yok sayar (tembel temizlik)
 * </pre>
 *
 * Bu tablo bir HIZLANDIRICI, gerçeğin kaynağı değil: yanlışsa mesaj kaybolmaz, istemci senkronla alır.
 */
@Component
public class RoutingTable {

    private static final RedisScript<Long> UNREGISTER = new DefaultRedisScript<>("""
            if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
              return redis.call('HDEL', KEYS[1], ARGV[1])
            end
            return 0""", Long.class);

    private final StringRedisTemplate redis;

    public RoutingTable(StringRedisTemplate redis) {
        this.redis = redis;
    }

    static String key(long userId) {
        return "conn:{" + userId + "}";
    }

    public void register(long userId, String deviceId, String instanceId) {
        redis.opsForHash().put(key(userId), deviceId, instanceId);
    }

    public void unregister(long userId, String deviceId, String instanceId) {
        redis.execute(UNREGISTER, List.of(key(userId)), deviceId, instanceId);
    }

    /** cihaz → gateway instance id */
    public Map<String, String> devices(long userId) {
        Map<Object, Object> raw = redis.opsForHash().entries(key(userId));
        Map<String, String> out = new java.util.HashMap<>();
        raw.forEach((k, v) -> out.put(k.toString(), v.toString()));
        return out;
    }
}
