package io.paramix.chat.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 1 yıldan eski mesajları küçük parçalar halinde siler (tek dev DELETE tabloyu ve replikasyonu kilitler).
 * Para isteğine bağlı mesajlar 5 yıl saklanır (denetim). Ölçek büyüyünce aylık partition + DROP PARTITION (README).
 */
@Component
@ConditionalOnProperty(name = "chat.roles.relay", havingValue = "true", matchIfMissing = true)
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);

    private final JdbcClient jdbc;

    public RetentionJob(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Scheduled(cron = "${chat.retention.cron:0 15 3 * * *}")
    public void purge() {
        int total = 0;
        int n;
        do {
            n = jdbc.sql("""
                            DELETE FROM messages WHERE ctid IN (
                              SELECT ctid FROM messages
                              WHERE payment_request_id IS NULL AND server_time < now() - interval '365 days'
                              LIMIT 5000)""").update();
            total += n;
        } while (n > 0);
        if (total > 0) {
            log.info("Saklama süresi dolan {} mesaj silindi", total);
        }
    }
}
