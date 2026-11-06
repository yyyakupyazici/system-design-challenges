package io.paramix.redislab.lock;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Tek-düğüm Redis kilidi: {@code SET key token NX PX ttl} + sahibine özel bırakma + fencing token.
 *
 * <p><b>Ne için uygun:</b> verimlilik (efficiency) kilidi — aynı pahalı işi iki pod yapmasın (rapor üretimi,
 * cache yükleme, cron). Kilit bazen iki kişide olursa sonuç "iş iki kez yapıldı" olur, felaket olmaz.
 *
 * <p><b>Ne için uygun değil:</b> doğruluk (correctness) kilidi — para, stok. Kilidin süresi dolarken sahibi
 * GC duraklamasında olabilir; uyandığında kilidin hâlâ kendisinde olduğunu sanar. Redis failover'da da
 * (asenkron replikasyon) yeni master kilidi hiç görmemiş olabilir. Bu yüzden doğruluk gereken yerde
 * kilit + {@link FencedStore fencing token} ya da veritabanının kendi garantisi (satır kilidi, unique kısıt) kullanılır.
 */
@Service
public class RedisLock {

    private static final RedisScript<Long> ACQUIRE = Scripts.load("lock_acquire", Long.class);
    private static final RedisScript<Long> RELEASE = Scripts.load("lock_release", Long.class);
    private static final RedisScript<Long> EXTEND = Scripts.load("lock_extend", Long.class);

    private final StringRedisTemplate redis;

    public RedisLock(StringRedisTemplate redis) {
        this.redis = redis;
    }

    static String lockKey(String resource) {
        return "lock:{" + resource + "}";
    }

    static String fenceKey(String resource) {
        return "lock:{" + resource + "}:fence";
    }

    public Optional<LockHandle> tryAcquire(String resource, Duration ttl) {
        String token = UUID.randomUUID().toString();
        Long fence = redis.execute(ACQUIRE, List.of(lockKey(resource), fenceKey(resource)),
                token, String.valueOf(ttl.toMillis()));
        return fence != null && fence > 0 ? Optional.of(new LockHandle(resource, token, fence)) : Optional.empty();
    }

    public boolean release(LockHandle h) {
        Long r = redis.execute(RELEASE, List.of(lockKey(h.resource())), h.token());
        return r != null && r == 1L;
    }

    public boolean extend(LockHandle h, Duration ttl) {
        Long r = redis.execute(EXTEND, List.of(lockKey(h.resource())), h.token(), String.valueOf(ttl.toMillis()));
        return r != null && r == 1L;
    }

    /** Kilidi alıp işi çalıştırır, her durumda bırakır. Kilit alınamazsa boş döner (iş başkasında). */
    public <T> Optional<T> withLock(String resource, Duration ttl, Function<LockHandle, T> work) {
        Optional<LockHandle> h = tryAcquire(resource, ttl);
        if (h.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(work.apply(h.get()));
        } finally {
            release(h.get());
        }
    }
}
