package io.paramix.provision.inbox;

import io.paramix.provision.config.ProvisionProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Saklama süresi dolan inbox kayıtlarını küçük partiler halinde siler (uzun kilit ve dev WAL yok).
 * Saklama süresi Kafka topic retention'ından uzun olmalı: Kafka'da hâlâ tekrar okunabilen bir mesajın kaydı
 * silinmişse, tekrar okuma onu ilk kez görür. CONFLICT kayıtları silinmez; onları bir insan kapatır.
 */
@Component
public class InboxCleanup {

    private static final Logger log = LoggerFactory.getLogger(InboxCleanup.class);
    private static final int BATCH = 5_000;

    private final JdbcTemplate jdbc;
    private final ProvisionProperties.Kafka props;

    public InboxCleanup(JdbcTemplate jdbc, ProvisionProperties props) {
        this.jdbc = jdbc;
        this.props = props.kafka();
    }

    @Scheduled(cron = "0 23 * * * *", zone = "UTC")
    public void cleanup() {
        int total = 0;
        int n;
        do {
            n = jdbc.update("""
                    DELETE FROM inbox WHERE ctid = ANY (ARRAY(
                        SELECT ctid FROM inbox
                        WHERE outcome = 'APPLIED' AND received_at < now() - make_interval(secs => ?)
                        LIMIT ?))
                    """, props.inboxRetention().toSeconds(), BATCH);
            total += n;
        } while (n == BATCH);
        if (total > 0) {
            log.info("Inbox temizliği: {} kayıt silindi", total);
        }
    }
}
