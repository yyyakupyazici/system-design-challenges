package io.paramix.provision.idempotency;

import java.sql.Date;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;

import io.paramix.provision.config.ProvisionProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Günlük idempotency partition'larını önceden açar, saklama süresi dolanları DROP eder.
 *
 * <p>DELETE yerine DROP: günde milyonlarca satırı DELETE etmek tabloda ölü satır (bloat) bırakır, autovacuum'u
 * yorar ve WAL üretir. DROP TABLE anlıktır.
 *
 * <p>Partition'lar birkaç gün önceden açılır: gece yarısına yakın, saati birkaç dakika ileri bir telefon "yarın"ın
 * partition'ına yazar. Açılmamış partition'a yazma hatası, gece 00:00'da bütün para hareketlerini durdururdu.
 * İş her pod'da çalışabilir; veritabanı fonksiyonu advisory lock ile tekrarı zararsız kılar.
 */
@Component
public class PartitionMaintenance {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenance.class);

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final ProvisionProperties.Idempotency props;

    public PartitionMaintenance(JdbcTemplate jdbc, Clock clock, ProvisionProperties props) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.props = props.idempotency();
    }

    @EventListener(ApplicationReadyEvent.class)
    @Scheduled(cron = "0 7 * * * *", zone = "UTC")
    public void maintain() {
        LocalDate today = LocalDate.now(clock.withZone(ZoneOffset.UTC));
        int created = ensure(today.minusDays(1), today.plusDays(props.partitionsAheadDays()));
        LocalDate cutoff = clock.instant().minus(props.retention()).atZone(ZoneOffset.UTC).toLocalDate();
        int dropped = dropBefore(cutoff);
        if (created + dropped > 0) {
            log.info("Idempotency partition bakımı: {} açıldı, {} silindi ({} öncesi)", created, dropped, cutoff);
        }
    }

    public int ensure(LocalDate from, LocalDate to) {
        return jdbc.queryForObject("SELECT idempotency_ensure_partitions(?, ?)", Integer.class,
                Date.valueOf(from), Date.valueOf(to));
    }

    public int dropBefore(LocalDate day) {
        return jdbc.queryForObject("SELECT idempotency_drop_partitions_before(?)", Integer.class, Date.valueOf(day));
    }
}
