package io.paramix.payreq.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.paramix.payreq.config.PayReqProperties;

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
 *   <li>Satırlar id sırasıyla okunur. Aynı isteğin geçişleri satır kilidiyle sıralı olduğu için o istek
 *       içinde id sırası = commit sırası. Dinleyen taraf ayrıca version ile eski olayı eler.</li>
 *   <li>Gönderim en az bir kez: Kafka'ya gidip silinemeden çöken satır tekrar gider. Fan-out tarafı zararsız
 *       (dinleyen version ile tekilleştirir).</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "payreq.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long LOCK_KEY = 7_007_002L;
    private static final int BATCH = 500;

    private final DataSourceProperties dsProps;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private Connection conn;
    private volatile boolean leader;

    public OutboxRelay(DataSourceProperties dsProps, KafkaTemplate<String, String> kafka, PayReqProperties props) {
        this.dsProps = dsProps;
        this.kafka = kafka;
        this.topic = props.topics().events();
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
                "SELECT id, event_key, payload FROM outbox ORDER BY id LIMIT " + BATCH)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                    sends.add(kafka.send(topic, rs.getString(2), rs.getString(3)));
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
