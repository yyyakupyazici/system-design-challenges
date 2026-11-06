package io.paramix.redislab.idempotency;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Idempotency key: istemci her işleme bir anahtar koyar ({@code Idempotency-Key} header'ı), ağ hatasında aynı
 * anahtarla tekrar dener. Sunucu aynı işi ikinci kez yapmaz, ilk sonucu döner.
 *
 * <pre>
 *  SET idem:{key} PENDING NX EX ttl   → ilk gelen alır, işi yapar, sonucu DONE:&lt;sonuç&gt; olarak yazar
 *  aynı anda ikinci istek              → PENDING görür → "işleniyor" (409)
 *  iş bittikten sonra tekrar           → DONE görür → kayıtlı sonucu döner (iş tekrar yapılmaz)
 *  iş hata verdi                       → anahtar silinir → istemci tekrar deneyebilir
 * </pre>
 *
 * <b>Sınır:</b> Redis'teki sonuç asenkron replikasyonla kaybolabilir (failover). Para gibi kritik işlemlerde
 * kalıcı kaynak veritabanındaki unique kısıttır; Redis önündeki hızlı kalkan.
 */
@Service
public class IdempotencyService {

    public sealed interface Outcome permits Executed, Replayed, InProgress {
    }

    public record Executed(String result) implements Outcome {
    }

    public record Replayed(String result) implements Outcome {
    }

    public record InProgress() implements Outcome {
    }

    private static final String PENDING = "PENDING";
    private static final String DONE = "DONE:";

    private final StringRedisTemplate redis;

    public IdempotencyService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Outcome execute(String idempotencyKey, Duration ttl, Supplier<String> action) {
        String key = "idem:{" + idempotencyKey + "}";
        if (Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, PENDING, ttl))) {
            String result;
            try {
                result = action.get();
            } catch (RuntimeException e) {
                redis.delete(key);
                throw e;
            }
            redis.opsForValue().set(key, DONE + result, ttl);
            return new Executed(result);
        }
        String v = redis.opsForValue().get(key);
        if (v != null && v.startsWith(DONE)) {
            return new Replayed(v.substring(DONE.length()));
        }
        return new InProgress();
    }
}
