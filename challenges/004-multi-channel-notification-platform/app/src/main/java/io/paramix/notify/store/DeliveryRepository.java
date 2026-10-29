package io.paramix.notify.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.Priority;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Teslimat durum makinesi. Her geçiş "beklenen eski durum" şartıyla yapılır (koşullu UPDATE):
 * iki pod aynı satırı aynı anda değiştiremez, geç gelen olaylar durumu geri alamaz.
 */
@Repository
public class DeliveryRepository {

    private static final String COLUMNS = """
            delivery_id, notification_id, user_id, channel, priority, campaign_id, recipient, title, body,
            status, provider, provider_msg_id, attempt, not_before, locked_until, last_error, updated_at""";

    private static final RowMapper<Delivery> MAPPER = DeliveryRepository::map;

    private final JdbcClient jdbc;

    public DeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Router aynı bildirimi iki kez işlese bile satır bir kez oluşur. */
    public boolean insertIfAbsent(Delivery d) {
        return jdbc.sql("""
                        INSERT INTO delivery (delivery_id, notification_id, user_id, channel, priority, campaign_id,
                                              recipient, title, body, status, not_before, last_error)
                        VALUES (:id, :nid, :uid, :channel, :priority, :cid, :recipient, :title, :body, :status, :notBefore, :err)
                        ON CONFLICT (delivery_id) DO NOTHING""")
                .param("id", d.deliveryId())
                .param("nid", d.notificationId())
                .param("uid", d.userId())
                .param("channel", d.channel().name())
                .param("priority", d.priority().name())
                .param("cid", d.campaignId())
                .param("recipient", d.recipient())
                .param("title", d.title())
                .param("body", d.body())
                .param("status", d.status().name())
                .param("notBefore", ts(d.notBefore()))
                .param("err", d.lastError())
                .update() == 1;
    }

    public Optional<Delivery> find(String deliveryId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM delivery WHERE delivery_id = :id")
                .param("id", deliveryId).query(MAPPER).optional();
    }

    public List<Delivery> findByNotification(String notificationId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM delivery WHERE notification_id = :nid ORDER BY delivery_id")
                .param("nid", notificationId).query(MAPPER).list();
    }

    /**
     * ③ Sahiplenme. PENDING ya da kilidi süresi dolmuş SENDING satırını bu worker'a kilitler.
     * Boş dönerse başka bir pod gönderiyor veya zaten gönderilmiş: mesaj atlanır.
     */
    public Optional<Delivery> claim(String deliveryId, Duration lease) {
        return jdbc.sql("""
                        UPDATE delivery
                        SET status = 'SENDING', attempt = attempt + 1, provider = NULL,
                            locked_until = now() + make_interval(secs => :lease), updated_at = now()
                        WHERE delivery_id = :id
                          AND (status = 'PENDING' OR (status = 'SENDING' AND locked_until < now()))
                        """ + " RETURNING " + COLUMNS)
                .param("id", deliveryId)
                .param("lease", lease.toMillis() / 1000.0)
                .query(MAPPER).optional();
    }

    /** Sağlayıcıyı çağırmadan HEMEN önce yazılır: pod ölürse reaper hangi sağlayıcıya soracağını bilir. */
    public void markSendingVia(String deliveryId, String provider) {
        jdbc.sql("UPDATE delivery SET provider = :p, updated_at = now() WHERE delivery_id = :id AND status = 'SENDING'")
                .param("p", provider).param("id", deliveryId).update();
    }

    public boolean markSent(String deliveryId, String provider, String providerMsgId, DeliveryStatus from) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'SENT', provider = :p, provider_msg_id = :mid,
                                            locked_until = NULL, last_error = NULL, updated_at = now()
                        WHERE delivery_id = :id AND status = :from""")
                .param("p", provider).param("mid", providerMsgId).param("id", deliveryId).param("from", from.name())
                .update() == 1;
    }

    public boolean markUnknown(String deliveryId, String provider, String error) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'UNKNOWN', provider = :p, last_error = :err,
                                            locked_until = NULL, updated_at = now()
                        WHERE delivery_id = :id AND status IN ('SENDING', 'UNKNOWN')""")
                .param("p", provider).param("err", error).param("id", deliveryId).update() == 1;
    }

    public boolean markFailed(String deliveryId, String error, DeliveryStatus... from) {
        return transition(deliveryId, DeliveryStatus.FAILED, error, from);
    }

    public boolean markSkipped(String deliveryId, String reason, DeliveryStatus... from) {
        return transition(deliveryId, DeliveryStatus.SKIPPED, reason, from);
    }

    /** Kota/sağlayıcı yok: satırı tekrar PENDING yap, Kafka mesajı yeniden denenecek. */
    public boolean releaseToPending(String deliveryId, String reason) {
        return transition(deliveryId, DeliveryStatus.PENDING, reason, DeliveryStatus.SENDING);
    }

    /** Reaper: kilit gerçekten dolmuşsa (arada başka worker yeniden sahiplenmediyse) PENDING'e döndür. */
    public boolean releaseExpiredLease(String deliveryId, String reason) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'PENDING', last_error = :err, locked_until = NULL, updated_at = now()
                        WHERE delivery_id = :id AND status = 'SENDING' AND locked_until < now() AND provider IS NULL""")
                .param("err", reason).param("id", deliveryId).update() == 1;
    }

    /** Reaper: gönderim sırasında pod öldü, kilit doldu → sonucu bilinmiyor. */
    public boolean markUnknownIfLeaseExpired(String deliveryId, String error) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'UNKNOWN', last_error = :err, locked_until = NULL, updated_at = now()
                        WHERE delivery_id = :id AND status = 'SENDING' AND locked_until < now() AND provider IS NOT NULL""")
                .param("err", error).param("id", deliveryId).update() == 1;
    }

    /** UNKNOWN / SCHEDULED / FALLBACK_WAIT → PENDING (yeniden yayınlanmadan önce). */
    public boolean promoteToPending(String deliveryId, DeliveryStatus from) {
        return transition(deliveryId, DeliveryStatus.PENDING, null, from);
    }

    private boolean transition(String deliveryId, DeliveryStatus to, String error, DeliveryStatus... from) {
        List<String> fromNames = java.util.Arrays.stream(from).map(Enum::name).toList();
        return jdbc.sql("""
                        UPDATE delivery SET status = :to, last_error = COALESCE(:err, last_error),
                                            locked_until = NULL, updated_at = now()
                        WHERE delivery_id = :id AND status IN (:from)""")
                .param("to", to.name()).param("err", error).param("id", deliveryId).param("from", fromNames)
                .update() == 1;
    }

    /** ④ Teslim raporu: durum sadece İLERİ gider; aynı rapor üç kez gelse de zararsız. */
    public boolean markDeliveredByProvider(String provider, String providerMsgId) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'DELIVERED', updated_at = now()
                        WHERE provider = :p AND provider_msg_id = :mid AND status IN ('SENT', 'UNKNOWN')""")
                .param("p", provider).param("mid", providerMsgId).update() == 1;
    }

    public boolean markFailedByProvider(String provider, String providerMsgId, String error) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'FAILED', last_error = :err, updated_at = now()
                        WHERE provider = :p AND provider_msg_id = :mid AND status IN ('SENT', 'UNKNOWN')""")
                .param("p", provider).param("mid", providerMsgId).param("err", error).update() == 1;
    }

    /** Uygulama push'u aldığında gönderdiği ack (push → SMS yedeğini iptal eder). */
    public boolean acknowledge(String deliveryId) {
        return jdbc.sql("""
                        UPDATE delivery SET status = 'DELIVERED', updated_at = now()
                        WHERE delivery_id = :id AND channel = 'PUSH' AND status IN ('SENDING', 'SENT', 'UNKNOWN')""")
                .param("id", deliveryId).update() == 1;
    }

    public List<Delivery> findDue(DeliveryStatus status, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM delivery WHERE status = :s AND not_before <= now()
                        ORDER BY not_before LIMIT :limit""")
                .param("s", status.name()).param("limit", limit).query(MAPPER).list();
    }

    /** Pod gönderirken öldü (kilit süresi doldu) ya da sonuç bilinmiyor ve inline reconcile başarısız oldu. */
    public List<Delivery> findStuck(Duration unknownGrace, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM delivery
                        WHERE (status = 'SENDING' AND locked_until < now())
                           OR (status = 'UNKNOWN' AND updated_at < now() - make_interval(secs => :grace))
                        ORDER BY updated_at LIMIT :limit""")
                .param("grace", unknownGrace.toMillis() / 1000.0).param("limit", limit).query(MAPPER).list();
    }

    private static Delivery map(ResultSet rs, int i) throws SQLException {
        return new Delivery(
                rs.getString("delivery_id"),
                rs.getString("notification_id"),
                rs.getString("user_id"),
                Channel.valueOf(rs.getString("channel")),
                Priority.valueOf(rs.getString("priority")),
                rs.getString("campaign_id"),
                rs.getString("recipient"),
                rs.getString("title"),
                rs.getString("body"),
                DeliveryStatus.valueOf(rs.getString("status")),
                rs.getString("provider"),
                rs.getString("provider_msg_id"),
                rs.getInt("attempt"),
                instant(rs.getTimestamp("not_before")),
                instant(rs.getTimestamp("locked_until")),
                rs.getString("last_error"),
                instant(rs.getTimestamp("updated_at")));
    }

    private static Instant instant(Timestamp t) {
        return t == null ? null : t.toInstant();
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }
}
