package io.paramix.notify.quota;

import java.util.Map;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.config.ProviderProperties;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * AIMD (Additive Increase, Multiplicative Decrease): kendi limitimiz sağlayıcının o anki gerçek
 * kapasitesiyle örtüşmeyebilir. 429 gelince efektif hızı hemen düşür (×0.7); bir süre 429 gelmezse
 * yavaşça (+50/sn) config değerine geri çık. Efektif hız Redis'te: tüm pod'lar birlikte yavaşlar.
 */
@Component
public class AimdController {

    private static final Logger log = LoggerFactory.getLogger(AimdController.class);

    private final StringRedisTemplate redis;
    private final NotifyProperties props;
    private final MeterRegistry meters;

    public AimdController(StringRedisTemplate redis, NotifyProperties props, MeterRegistry meters) {
        this.redis = redis;
        this.props = props;
        this.meters = meters;
    }

    public void onRateLimited(String provider) {
        ProviderProperties pp = props.providers().get(provider);
        String prefix = QuotaService.keyPrefix(provider);
        try {
            double current = currentRate(prefix, pp);
            long next = Math.max(Math.max(1, pp.globalRate() / 10), Math.round(current * props.quota().aimdDecrease()));
            redis.opsForValue().set(prefix + "rate", String.valueOf(next));
            redis.opsForValue().set(prefix + "last429", String.valueOf(System.currentTimeMillis()));
            meters.counter("notify.provider.rate_limited", "provider", provider).increment();
            log.warn("{} 429 döndü, efektif hız {} → {}/sn", provider, Math.round(current), next);
        } catch (DataAccessException e) {
            log.warn("AIMD güncellenemedi: {}", e.getMessage());
        }
    }

    public double effectiveRate(String provider) {
        ProviderProperties pp = props.providers().get(provider);
        return currentRate(QuotaService.keyPrefix(provider), pp);
    }

    @Scheduled(fixedDelayString = "${notify.quota.aimd-tick-ms:10000}")
    public void recover() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ProviderProperties> e : props.providers().entrySet()) {
            String prefix = QuotaService.keyPrefix(e.getKey());
            try {
                String rate = redis.opsForValue().get(prefix + "rate");
                if (rate == null) {
                    continue;
                }
                String last = redis.opsForValue().get(prefix + "last429");
                if (last != null && now - Long.parseLong(last) < props.quota().aimdRecoverAfter().toMillis()) {
                    continue;
                }
                long next = Math.round(Double.parseDouble(rate)) + props.quota().aimdIncreaseStep();
                if (next >= e.getValue().globalRate()) {
                    redis.delete(prefix + "rate");
                } else {
                    redis.opsForValue().set(prefix + "rate", String.valueOf(next));
                }
            } catch (DataAccessException ex) {
                log.debug("AIMD toparlanma atlandı: {}", ex.getMessage());
            }
        }
    }

    private double currentRate(String prefix, ProviderProperties pp) {
        String v = redis.opsForValue().get(prefix + "rate");
        return v == null ? pp.globalRate() : Math.min(pp.globalRate(), Double.parseDouble(v));
    }
}
