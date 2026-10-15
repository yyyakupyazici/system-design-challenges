package io.paylink.gateway.policy;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * Kural değişikliklerini tüm gateway pod'larına yayar: admin API değişikliği DB'ye yazar,
 * sonra Redis Pub/Sub kanalına bir mesaj atar; her pod mesajı alınca kuralları yeniden yükler.
 *
 * <p>Pub/Sub "en fazla bir kez" teslim eder; kaçan mesajları {@link PolicyCache#refreshIfChanged()} yakalar.
 */
@Component
public class PolicyChangeSubscriber implements ApplicationRunner, DisposableBean {

    public static final String CHANNEL = "ratelimit:policy-changed";
    private static final Logger log = LoggerFactory.getLogger(PolicyChangeSubscriber.class);

    private final ReactiveStringRedisTemplate redis;
    private final PolicyCache cache;
    private Disposable subscription;

    public PolicyChangeSubscriber(ReactiveStringRedisTemplate redis, PolicyCache cache) {
        this.redis = redis;
        this.cache = cache;
    }

    @Override
    public void run(ApplicationArguments args) {
        subscription = redis.listenToChannel(CHANNEL)
                .concatMap(msg -> Mono.fromRunnable(this::safeReload).subscribeOn(Schedulers.boundedElastic()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(s -> log.warn("Pub/Sub bağlantısı koptu, yeniden deneniyor: {}", s.failure().getMessage())))
                .subscribe();
    }

    /** Admin API'den çağrılır. */
    public Mono<Long> publishChange(String description) {
        return redis.convertAndSend(CHANNEL, description);
    }

    private void safeReload() {
        try {
            cache.reload();
        } catch (RuntimeException e) {
            log.warn("Değişiklik olayı sonrası yükleme başarısız; periyodik yenileme tekrar deneyecek: {}", e.getMessage());
        }
    }

    @Override
    public void destroy() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
