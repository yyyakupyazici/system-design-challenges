package io.paramix.provision.outbox;

import java.util.List;
import java.util.UUID;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.outbox.EventPublisher.OutboxEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox → yayıncı. Her tur bir transaction:
 * <ol>
 *   <li>{@code pg_try_advisory_xact_lock}: aynı anda tek relay çalışır (pod sayısından bağımsız). Bu, aynı provizyonun
 *       olaylarının id sırasıyla yayınlanmasını garanti eder. Kilit transaction'la birlikte kalkar; sahibi ölürse
 *       sonraki turda başka pod alır.</li>
 *   <li>Satırları id sırasıyla oku, yayınla, sil. Yayın hata verirse transaction geri alınır, satırlar yerinde
 *       kalır ve tekrar gönderilir (en az bir kez).</li>
 * </ol>
 * Buradaki transaction iş satırlarını değil, sadece outbox satırlarını kilitler; yayıncıyı beklemesi para
 * hareketlerini bekletmez. Bekleme Kafka'nın delivery.timeout.ms'i ile sınırlı ve PostgreSQL'in
 * idle_in_transaction_session_timeout'undan kısa tutulur (ikisi application.yml'de birlikte ayarlanır).
 */
@Component
@ConditionalOnProperty(name = "provision.outbox.relay-enabled", havingValue = "true")
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long LOCK_KEY = 8_008_001L;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventPublisher publisher;
    private final int batchSize;

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, EventPublisher publisher, ProvisionProperties props) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.publisher = publisher;
        this.batchSize = props.outbox().batchSize();
    }

    @Scheduled(fixedDelayString = "${provision.outbox.interval:200ms}")
    public void tick() {
        try {
            while (drainOnce() == batchSize) {
                // birikme varsa beklemeden devam
            }
        } catch (RuntimeException e) {
            log.warn("Outbox relay turu başarısız, tekrar denenecek: {}", e.toString());
        }
    }

    /** @return yayınlanan olay sayısı */
    public int drainOnce() {
        Integer published = tx.execute(status -> {
            Boolean locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class, LOCK_KEY);
            if (!Boolean.TRUE.equals(locked)) {
                return 0;
            }
            List<Row> rows = jdbc.query("""
                            SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload::text, created_at
                            FROM outbox ORDER BY id LIMIT ?
                            """,
                    (rs, i) -> new Row(rs.getLong(1), new OutboxEvent(rs.getObject(2, UUID.class), rs.getString(3),
                            rs.getObject(4, UUID.class), rs.getString(5), rs.getString(6),
                            rs.getTimestamp(7).toInstant())),
                    batchSize);
            if (!rows.isEmpty()) {
                publisher.publish(rows.stream().map(Row::event).toList());
            }
            if (!rows.isEmpty()) {
                // "id <= son okunan" ile silmek YANLIŞ olurdu: identity değeri INSERT anında alınır, commit sırası
                // farklı olabilir. Daha küçük id'li ama henüz commit edilmemiş bir olay yayınlanmadan silinirdi.
                Long[] ids = rows.stream().map(Row::id).toArray(Long[]::new);
                jdbc.update("DELETE FROM outbox WHERE id = ANY (?)", (Object) ids);
            }
            return rows.size();
        });
        return published == null ? 0 : published;
    }

    private record Row(long id, OutboxEvent event) {
    }
}
