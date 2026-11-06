package io.paramix.redislab.lock;

import java.util.List;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Fencing token'ı kontrol eden depolama örneği. Gerçek hayatta bu kontrol veritabanında yapılır:
 * {@code UPDATE ... SET value = ?, fence = ? WHERE id = ? AND fence <= ?}. Kilit "kimin sırası" der,
 * fencing "geç kalmış eski sahip yazamaz" der. Doğruluk için ikisi birlikte gerekir.
 */
@Service
public class FencedStore {

    private static final RedisScript<Long> WRITE = Scripts.load("fenced_write", Long.class);

    private final StringRedisTemplate redis;

    public FencedStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean write(String resource, long fencingToken, String value) {
        Long r = redis.execute(WRITE, List.of("data:{" + resource + "}"), String.valueOf(fencingToken), value);
        return r != null && r == 1L;
    }

    public String read(String resource) {
        Object v = redis.opsForHash().get("data:{" + resource + "}", "value");
        return v == null ? null : v.toString();
    }
}
