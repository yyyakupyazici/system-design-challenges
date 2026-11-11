package io.paramix.provision.reconciliation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Mutabakat: sistemin kendi kendini denetlemesi. Idempotency ve kısıtlar hataları ÖNLER; mutabakat önlenemeyeni
 * (bug, elle yapılmış SQL, yeni bir yarış) YAKALAR. Para sistemi için ikisi birlikte gerekli.
 *
 * <ol>
 *   <li>{@code balances.held} = o müşterinin HELD provizyonlarının toplamı</li>
 *   <li>{@code balances.available/held} = ledger'daki AVAILABLE/HELD hareketlerinin toplamı</li>
 *   <li>Her journal (varlık başına) toplamı sıfır</li>
 * </ol>
 * REPEATABLE READ: üç sorgu aynı anlık görüntüyü (snapshot) görür, arada commit olan işlem yanlış alarm üretmez.
 * Üretimde bu sorgular okuma replikasında ve artımlı (son kontrol noktasından beri) çalışır.
 */
@Component
public class Reconciliation {

    private static final Logger log = LoggerFactory.getLogger(Reconciliation.class);

    public record Discrepancy(String check, Map<String, Object> details) {
    }

    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;

    public Reconciliation(JdbcTemplate jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.meters = meters;
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<Discrepancy> run() {
        List<Discrepancy> found = new ArrayList<>();
        jdbc.queryForList("""
                SELECT COALESCE(b.customer_id, p.customer_id) AS customer_id, COALESCE(b.asset, p.asset) AS asset,
                       COALESCE(b.held, 0) AS balance_held, COALESCE(p.total, 0) AS provisions_held
                FROM balances b
                FULL JOIN (SELECT customer_id, asset, SUM(amount) AS total
                           FROM provisions WHERE status = 'HELD' GROUP BY customer_id, asset) p
                  ON p.customer_id = b.customer_id AND p.asset = b.asset
                WHERE COALESCE(b.held, 0) <> COALESCE(p.total, 0)
                """).forEach(row -> found.add(new Discrepancy("held_vs_provisions", row)));
        jdbc.queryForList("""
                SELECT COALESCE(b.customer_id, l.customer_id) AS customer_id, COALESCE(b.asset, l.asset) AS asset,
                       COALESCE(b.available, 0) AS balance_available, COALESCE(l.available, 0) AS ledger_available,
                       COALESCE(b.held, 0) AS balance_held, COALESCE(l.held, 0) AS ledger_held
                FROM balances b
                FULL JOIN (SELECT customer_id, asset,
                                  COALESCE(SUM(delta) FILTER (WHERE bucket = 'AVAILABLE'), 0) AS available,
                                  COALESCE(SUM(delta) FILTER (WHERE bucket = 'HELD'), 0) AS held
                           FROM ledger_entries GROUP BY customer_id, asset) l
                  ON l.customer_id = b.customer_id AND l.asset = b.asset
                WHERE COALESCE(b.available, 0) <> COALESCE(l.available, 0)
                   OR COALESCE(b.held, 0) <> COALESCE(l.held, 0)
                """).forEach(row -> found.add(new Discrepancy("balance_vs_ledger", row)));
        jdbc.queryForList("""
                SELECT journal_id, asset, SUM(delta) AS total
                FROM ledger_entries GROUP BY journal_id, asset HAVING SUM(delta) <> 0
                """).forEach(row -> found.add(new Discrepancy("unbalanced_journal", row)));
        return found;
    }

    @Scheduled(fixedDelayString = "${provision.reconciliation.interval:5m}", initialDelayString = "1m")
    public void scheduled() {
        List<Discrepancy> found = run();
        meters.counter("reconciliation.discrepancies").increment(found.size());
        if (!found.isEmpty()) {
            log.error("MUTABAKAT FARKI ({} adet): {}", found.size(), found);
        }
    }
}
