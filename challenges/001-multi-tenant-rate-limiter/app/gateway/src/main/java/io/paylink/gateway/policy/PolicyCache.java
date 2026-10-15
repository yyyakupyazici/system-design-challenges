package io.paylink.gateway.policy;

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Kuralların her pod'daki bellek kopyası ("ders kitabı").
 *
 * <ul>
 *   <li>Başlangıçta DB'den yüklenir (readiness ancak bundan sonra UP olur).</li>
 *   <li>Değişiklik olayı gelince ({@link PolicyChangeSubscriber}) hemen yeniden yüklenir → 1-2 sn.</li>
 *   <li>Olay kaçarsa güvenlik ağı: her {@code ratelimit.policy-refresh-interval}'de versiyon kontrolü.</li>
 *   <li>DB çökerse son bilinen snapshot ile çalışmaya devam edilir.</li>
 * </ul>
 */
@Component
public class PolicyCache implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(PolicyCache.class);

    private final PolicyRepository repository;
    private final AtomicReference<PolicySnapshot> current = new AtomicReference<>(PolicySnapshot.empty());

    public PolicyCache(PolicyRepository repository) {
        this.repository = repository;
    }

    public PolicySnapshot current() {
        return current.get();
    }

    @Override
    public void run(ApplicationArguments args) {
        reload();
    }

    /** Bloklayan çağrı; event loop'ta çağırma. */
    public synchronized void reload() {
        long version = repository.maxVersion();
        PolicySnapshot snapshot = PolicySnapshot.of(version, repository.findAllApiKeys(), repository.findAllPolicies());
        current.set(snapshot);
        log.info("Rate limit kuralları yüklendi: version={}, policies={}, apiKeys={}",
                version, snapshot.allPolicies().size(), snapshot.apiKeyCount());
    }

    @Scheduled(fixedDelayString = "${ratelimit.policy-refresh-interval:PT60S}",
               initialDelayString = "${ratelimit.policy-refresh-interval:PT60S}")
    public void refreshIfChanged() {
        try {
            long dbVersion = repository.maxVersion();
            if (dbVersion != current().version()) {
                log.info("Kaçırılmış değişiklik tespit edildi ({} → {}), yeniden yükleniyor", current().version(), dbVersion);
                reload();
            }
        } catch (RuntimeException e) {
            log.warn("Kural yenileme başarısız, son bilinen kurallarla devam ediliyor: {}", e.getMessage());
        }
    }
}
