package io.paramix.notify.quota;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.config.ProviderProperties;
import io.paramix.notify.domain.Priority;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

/**
 * {@code acquire}: sağlayıcıyı çağırmadan önce Redis'ten izin al. Jeton yoksa sağlayıcıya hiç gidilmez → 429 yok.
 *
 * <p>Redis'e ulaşılamazsa öncelik bazlı davranış:
 * CRITICAL/TRANSACTIONAL → pod içi yerel kovaya geç (garanti / pod sayısı); MARKETING → dur (fail-closed).
 */
@Service
public class QuotaService {

    private static final Logger log = LoggerFactory.getLogger(QuotaService.class);

    private final StringRedisTemplate redis;
    private final NotifyProperties props;
    private final MeterRegistry meters;
    private final DefaultRedisScript<List> script;
    private final Map<String, LocalTokenBucket> localBuckets = new ConcurrentHashMap<>();

    public QuotaService(StringRedisTemplate redis, NotifyProperties props, MeterRegistry meters) {
        this.redis = redis;
        this.props = props;
        this.meters = meters;
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("scripts/priority_quota.lua"));
        this.script.setResultType(List.class);
    }

    public static String keyPrefix(String provider) {
        return "q:{" + provider + "}:";   // hash tag → tüm kovalar aynı Redis Cluster slot'unda
    }

    public QuotaDecision tryAcquire(String provider, Priority priority) {
        ProviderProperties pp = config(provider);
        ProviderProperties.Share share = pp.share(priority);
        String prefix = keyPrefix(provider);
        QuotaDecision d;
        try {
            List<?> r = redis.execute(script,
                    List.of(prefix + "shared", prefix + priority + ":res", prefix + priority + ":ceil", prefix + "rate"),
                    String.valueOf(pp.globalRate()),
                    String.valueOf(pp.reservedSum()),
                    String.valueOf(share.reserved()),
                    String.valueOf(share.ceiling()),
                    "1",
                    String.valueOf(props.quota().keyTtl().toMillis()));
            long allowed = ((Number) r.get(0)).longValue();
            long wait = ((Number) r.get(1)).longValue();
            long source = ((Number) r.get(2)).longValue();
            d = allowed == 1
                    ? new QuotaDecision(true, 0, source == 1 ? QuotaDecision.Source.RESERVED : QuotaDecision.Source.SHARED)
                    : QuotaDecision.denied(wait);
        } catch (DataAccessException e) {
            d = localFallback(provider, priority, pp);
            log.warn("Kota Redis'ine ulaşılamadı ({}), yerel karar: {} {} → {}", e.getClass().getSimpleName(),
                    provider, priority, d.allowed());
        }
        meters.counter("notify.quota.acquire", "provider", provider, "priority", priority.name(),
                "result", d.source().name()).increment();
        return d;
    }

    /**
     * maxWait boyunca, Lua'nın söylediği süre kadar uyuyarak tekrar dener (Redis'i boşuna meşgul etmez).
     * Worker thread'i beklerken Kafka partition'ı da bekler: doğal backpressure.
     */
    public QuotaDecision acquireWithin(String provider, Priority priority, Duration maxWait) {
        long deadline = System.nanoTime() + maxWait.toNanos();
        while (true) {
            QuotaDecision d = tryAcquire(provider, priority);
            if (d.allowed()) {
                return d;
            }
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
            if (remainingMs <= 0) {
                return d;
            }
            try {
                Thread.sleep(Math.min(d.waitMs(), remainingMs));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return d;
            }
        }
    }

    private QuotaDecision localFallback(String provider, Priority priority, ProviderProperties pp) {
        if (priority == Priority.MARKETING) {
            return QuotaDecision.denied(1000);
        }
        int pods = Math.max(1, props.quota().fallbackPodCount());
        double rate = Math.max(1, (double) pp.share(priority).reserved() / pods);
        LocalTokenBucket bucket = localBuckets.computeIfAbsent(provider + ":" + priority,
                k -> new LocalTokenBucket(rate, System::nanoTime));
        long wait = bucket.tryAcquire();
        return wait == 0 ? new QuotaDecision(true, 0, QuotaDecision.Source.LOCAL_FALLBACK) : QuotaDecision.denied(wait);
    }

    private ProviderProperties config(String provider) {
        ProviderProperties pp = props.providers().get(provider);
        if (pp == null) {
            throw new IllegalArgumentException("Kota tanımı yok: " + provider);
        }
        return pp;
    }
}
