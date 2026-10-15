package io.paylink.gateway.limiter;

import io.paylink.gateway.config.RateLimitProperties;
import io.paylink.gateway.policy.PolicyResolver.Resolution;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Kovaları Redis'te tutan dağıtık rate limiter ("ortak kara tahta").
 *
 * <ul>
 *   <li>Tenant kovaları + günlük kota: tek Lua çağrısı, hepsi ya da hiçbiri.</li>
 *   <li>Global kova: ayrı çağrı (farklı Redis Cluster slot'unda yaşar). Sadece tenant kontrolünü
 *       geçen isteklerde çalışır.</li>
 *   <li>Redis hatası / timeout: {@code ratelimit.fail-open}'a göre karar (Adım 5'te derinleşecek).</li>
 * </ul>
 */
@Component
public class RedisRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);
    private static final String GLOBAL_KEY = "rl:{global}:all";

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SCRIPT =
            RedisScript.of(new ClassPathResource("scripts/token_bucket.lua"), List.class);

    private final ReactiveStringRedisTemplate redis;
    private final RateLimitProperties props;

    public RedisRateLimiter(ReactiveStringRedisTemplate redis, RateLimitProperties props) {
        this.redis = redis;
        this.props = props;
    }

    /** Tenant kovaları (genel + endpoint) ve günlük kota. */
    public Mono<RateLimitDecision> checkTenant(Resolution resolution, LocalDate todayUtc) {
        List<String> keys = new ArrayList<>();
        List<String> args = new ArrayList<>();
        args.add(String.valueOf(resolution.buckets().size()));
        for (BucketSpec bucket : resolution.buckets()) {
            keys.add(bucket.redisKey());
            args.add(String.valueOf(bucket.ratePerSec()));
            args.add(String.valueOf(bucket.burst()));
        }
        String quotaKey = null;
        if (resolution.dailyQuota() != null) {
            quotaKey = "quota:" + resolution.hashTag() + ":" + todayUtc;
            keys.add(quotaKey);
            args.add(String.valueOf(resolution.dailyQuota()));
        } else {
            args.add("-1");
        }
        args.add(String.valueOf(props.quotaKeyTtl().toSeconds()));

        return execute(keys, args, quotaKey);
    }

    /** Tüm sistemi koruyan global kova. Kota yok. */
    public Mono<RateLimitDecision> checkGlobal() {
        RateLimitProperties.Global global = props.global();
        if (!global.enabled()) {
            return Mono.just(new RateLimitDecision(true, RateLimitDecision.Reason.OK, -1, -1, 0, 0, -1, null));
        }
        List<String> args = List.of("1", String.valueOf(global.ratePerSec()), String.valueOf(global.burst()),
                "-1", "0");
        return execute(List.of(GLOBAL_KEY), args, null)
                .map(d -> d.allowed() ? d : d.asGlobalRejection());
    }

    /**
     * 5xx dönen istek kotadan düşmesin (refund). Kova jetonu iade edilmez: kova altyapıyı korur,
     * istek altyapıyı gerçekten kullandı. Hata yutulur; en kötü ihtimalle müşteri 1 kota kaybeder.
     */
    public Mono<Void> refundQuota(RateLimitDecision decision) {
        if (decision.quotaKey() == null) {
            return Mono.empty();
        }
        return redis.opsForValue().decrement(decision.quotaKey())
                .timeout(props.redisTimeout())
                .onErrorResume(e -> {
                    log.debug("Kota iadesi başarısız: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    @SuppressWarnings("unchecked")
    private Mono<RateLimitDecision> execute(List<String> keys, List<String> args, String quotaKey) {
        return redis.execute(SCRIPT, keys, args)
                .next()
                .map(result -> RateLimitDecision.fromScript((List<Object>) result, quotaKey))
                .timeout(props.redisTimeout())
                .onErrorResume(e -> {
                    log.warn("Rate limit store erişilemedi ({}), fail-open={}", e.toString(), props.failOpen());
                    return Mono.just(props.failOpen() ? RateLimitDecision.failOpen() : RateLimitDecision.failClosed());
                });
    }
}
