package io.paramix.chat.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.paramix.chat.config.ChatProperties;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Outbox → Kafka. Aktif–bekleyen (active/standby): bütün relay pod'ları aynı advisory lock'u dener, alan lider olur,
 * diğerleri bekler. Lider ölünce bağlantısı kopar, PostgreSQL kilidi kendiliğinden bırakır, bekleyen devralır.
 *
 * <ul>
 *   <li>Kilit ve iş AYNI, havuz dışı bağlantıda: bağlantı koparsa lider iş yapamaz hale gelir (iki lider olmaz).</li>
 *   <li>Satırlar id sırasıyla okunur. Aynı sohbetin mesajları sohbet satırı kilitlenerek yazıldığı için o sohbet
 *       içinde id sırası = commit sırası; sohbetler arası sıra önemli değil.</li>
 *   <li>Gönderim en az bir kez: Kafka'ya gidip silinemeden çöken satır tekrar gider. Fan-out tarafı zararsız
 *       (istemci seq ile tekilleştirir).</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "chat.roles.relay", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long LOCK_KEY = 7_007_001L;
    private static final int BATCH = 500;

    private final DataSourceProperties dsProps;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private Connection conn;
    private volatile boolean leader;

    public OutboxRelay(DataSourceProperties dsProps, KafkaTemplate<String, String> kafka, ChatProperties props) {
        this.dsProps = dsProps;
        this.kafka = kafka;
        this.topic = props.kafka().topic();
    }

    public boolean isLeader() {
        return leader;
    }

    @Scheduled(fixedDelay = 50)
    public void tick() {
        try {
            if (!leader && !tryBecomeLeader()) {
                return;
            }
            while (drainBatch() == BATCH) {
                // birikme varsa beklemeden devam
            }
        } catch (Exception e) {
            log.warn("Outbox relay hatası, liderlik bırakılıyor: {}", e.toString());
            resetConnection();
        }
    }

    private boolean tryBecomeLeader() throws SQLException {
        if (conn == null || conn.isClosed()) {
            conn = java.sql.DriverManager.getConnection(dsProps.determineUrl(), dsProps.determineUsername(),
                    dsProps.determinePassword());
            conn.setAutoCommit(true);
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            ps.setLong(1, LOCK_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                leader = rs.getBoolean(1);
            }
        }
        if (leader) {
            log.info("Outbox relay lideri bu pod");
        }
        return leader;
    }

    private int drainBatch() throws Exception {
        List<Long> ids = new ArrayList<>();
        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, conversation_id, payload FROM outbox ORDER BY id LIMIT " + BATCH)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                    sends.add(kafka.send(topic, String.valueOf(rs.getLong(2)), rs.getString(3)));
                }
            }
        }
        if (ids.isEmpty()) {
            return 0;
        }
        kafka.flush();
        for (CompletableFuture<SendResult<String, String>> f : sends) {
            f.get(10, TimeUnit.SECONDS);                 // biri bile başarısızsa hiçbiri silinmez, tekrar gönderilir
        }
        try (PreparedStatement del = conn.prepareStatement("DELETE FROM outbox WHERE id = ANY (?)")) {
            del.setArray(1, conn.createArrayOf("bigint", ids.toArray()));
            del.executeUpdate();
        }
        return ids.size();
    }

    private void resetConnection() {
        leader = false;
        try {
            if (conn != null) {
                conn.close();                            // kilit de bağlantıyla birlikte düşer
            }
        } catch (SQLException ignored) {
            // zaten kopuk
        }
        conn = null;
    }

    @PreDestroy
    public void shutdown() {
        resetConnection();
    }
}
