package io.paramix.redislab.hotkey;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Okuma tarafı sıcak key çözümü: çok kısa TTL'li yerel cache. Kampanya sayfasının config'i saniyede 200 bin kez
 * okunuyorsa, 50 pod × 1 sn TTL = Redis'e saniyede en fazla 50 okuma gider. Bedel: değişiklik en fazla 1 sn geç görünür.
 * Daha hızlı yayılması gerekiyorsa {@link io.paramix.redislab.pubsub.NearCache} gibi pub/sub ile silinir.
 */
@Service
public class HotReadCache {

    private record Entry(String value, long expiresAt) {
    }

    private final Map<String, Entry> local = new ConcurrentHashMap<>();
    private final AtomicLong redisReads = new AtomicLong();
    private final StringRedisTemplate redis;

    public HotReadCache(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public String get(String key, Duration localTtl) {
        long now = System.currentTimeMillis();
        Entry e = local.get(key);
        if (e != null && e.expiresAt() > now) {
            return e.value();
        }
        redisReads.incrementAndGet();
        String v = redis.opsForValue().get(key);
        local.put(key, new Entry(v, now + localTtl.toMillis()));
        return v;
    }

    public long redisReads() {
        return redisReads.get();
    }
}
