package io.paramix.redislab.cache;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.paramix.redislab.common.Scripts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Cache-aside (lazy loading) ve üç klasik cache felaketine karşı savunma:
 *
 * <ul>
 *   <li><b>Stampede (thundering herd):</b> popüler bir key düştü, 1.000 istek aynı anda DB'ye gitti.
 *       Savunma: sadece kilidi alan yükler, diğerleri kısa süre bekleyip cache'ten okur.</li>
 *   <li><b>Avalanche:</b> aynı anda yazılan binlerce key aynı anda düşer. Savunma: TTL'ye rastgele sapma (jitter).</li>
 *   <li><b>Penetration:</b> olmayan bir id ile sürekli sorgu, her seferinde DB'ye gider. Savunma: "yok" bilgisini de
 *       kısa süreliğine cache'le (negative caching).</li>
 * </ul>
 *
 * Güncellemede cache'e yazmak yerine SİLİYORUZ: iki eşzamanlı güncelleme ters sırada cache'e yazarsa cache eski
 * değerde kalır; silmek bu yarışı daraltır (bir sonraki okuma DB'den taze değeri getirir).
 */
@Service
public class CacheAsideService {

    static final String NULL_MARKER = "__null__";
    private static final RedisScript<Long> RELEASE = Scripts.load("lock_release", Long.class);

    private final StringRedisTemplate redis;
    private final ProductRepository repository;
    private final ObjectMapper json;
    private final Duration ttl;
    private final Duration jitter;
    private final Duration negativeTtl;

    public CacheAsideService(StringRedisTemplate redis, ProductRepository repository, ObjectMapper json,
                             @Value("${lab.cache.ttl:10m}") Duration ttl,
                             @Value("${lab.cache.ttl-jitter:2m}") Duration jitter,
                             @Value("${lab.cache.negative-ttl:1m}") Duration negativeTtl) {
        this.redis = redis;
        this.repository = repository;
        this.json = json;
        this.ttl = ttl;
        this.jitter = jitter;
        this.negativeTtl = negativeTtl;
    }

    static String key(String id) {
        return "product:{" + id + "}";
    }

    public Optional<Product> get(String id) {
        String key = key(id);
        String cached = redis.opsForValue().get(key);
        if (cached != null) {
            return decode(cached);
        }
        // Iska. Yükleme hakkı sadece kilidi alana: 1.000 eşzamanlı ıskada DB'ye 1 sorgu gider.
        String lockKey = key + ":lock";                       // aynı hash tag → aynı slot
        String token = UUID.randomUUID().toString();
        Boolean mine = redis.opsForValue().setIfAbsent(lockKey, token, Duration.ofSeconds(3));
        if (Boolean.TRUE.equals(mine)) {
            try {
                Optional<Product> fresh = repository.findById(id);
                write(key, fresh);
                return fresh;
            } finally {
                redis.execute(RELEASE, List.of(lockKey), token);
            }
        }
        // Başkası yüklüyor: kısa aralıklarla cache'e bak.
        for (int i = 0; i < 100; i++) {
            sleep(20);
            String v = redis.opsForValue().get(key);
            if (v != null) {
                return decode(v);
            }
        }
        // 2 sn içinde gelmediyse (yükleyen öldü) kendimiz okuruz: cache yardımcıdır, kullanılabilirliği engellememeli.
        return repository.findById(id);
    }

    public void update(Product p) {
        repository.save(p);
        redis.delete(key(p.id()));
    }

    private void write(String key, Optional<Product> value) {
        if (value.isEmpty()) {
            redis.opsForValue().set(key, NULL_MARKER, negativeTtl);
            return;
        }
        long extra = jitter.isZero() ? 0 : ThreadLocalRandom.current().nextLong(jitter.toMillis());
        redis.opsForValue().set(key, encode(value.get()), ttl.plusMillis(extra));
    }

    private Optional<Product> decode(String v) {
        if (NULL_MARKER.equals(v)) {
            return Optional.empty();
        }
        try {
            return Optional.of(json.readValue(v, Product.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private String encode(Product p) {
        try {
            return json.writeValueAsString(p);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
