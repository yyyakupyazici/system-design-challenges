package io.paramix.redislab.atomic;

import java.util.List;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Atomikliğin üç seviyesi. Hepsi "bir sayacı artır" işini yapar, sadece biri doğrudur.
 *
 * <ol>
 *   <li>{@link #naiveIncrement}: GET + SET. İki istemci aynı değeri okursa artışlardan biri kaybolur (lost update).</li>
 *   <li>{@link #atomicIncrement}: INCR. Tek komut, Redis tek thread'de sırayla çalıştırır, yarış yok.</li>
 *   <li>{@link #incrementIfBelow}: "limit altındaysa artır". Tek komut yetmez (koşul var) → Lua script'i.</li>
 * </ol>
 */
@Service
public class CounterService {

    private static final RedisScript<Long> INCR_IF_BELOW = Scripts.load("incr_if_below", Long.class);

    private final StringRedisTemplate redis;

    public CounterService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** YANLIŞ: oku-değiştir-yaz iki ayrı ağ çağrısı. Arada başka bir istemci araya girebilir. */
    public long naiveIncrement(String key) {
        String v = redis.opsForValue().get(key);
        long next = (v == null ? 0 : Long.parseLong(v)) + 1;
        redis.opsForValue().set(key, String.valueOf(next));
        return next;
    }

    /** DOĞRU: INCR atomiktir. Sayaç, ziyaret sayısı, id üretimi. */
    public long atomicIncrement(String key) {
        return redis.opsForValue().increment(key);
    }

    /** DOĞRU: koşullu işlem Lua'da. Kampanya katılımcı sınırı, deneme hakkı, kota. */
    public boolean incrementIfBelow(String key, long limit) {
        Long r = redis.execute(INCR_IF_BELOW, List.of(key), String.valueOf(limit));
        return r != null && r == 1L;
    }

    public long get(String key) {
        String v = redis.opsForValue().get(key);
        return v == null ? 0 : Long.parseLong(v);
    }
}
