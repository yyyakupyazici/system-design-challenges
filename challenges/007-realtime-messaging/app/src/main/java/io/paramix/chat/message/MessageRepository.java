package io.paramix.chat.message;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class MessageRepository {

    private static final String SELECT = """
            SELECT m.conversation_id, m.seq, m.sender_id, m.client_msg_id, m.kind, m.body, m.server_time,
                   p.id AS pr_id, p.status AS pr_status, p.amount_kurus AS pr_amount, p.payer_id AS pr_payer
            FROM messages m LEFT JOIN payment_requests p ON p.id = m.payment_request_id
            """;

    private final JdbcClient jdbc;

    public MessageRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Sohbetin sayacını artırır ve yeni numarayı döner. Satır kilidi: aynı sohbete aynı anda yazan iki istek
     * sırayla geçer; transaction bitene kadar ikincisi bekler. Böylece numaralar boşluksuz ve herkes için aynı.
     */
    public long nextSeq(long conversationId) {
        return jdbc.sql("UPDATE conversations SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq")
                .param(conversationId).query(Long.class).single();
    }

    /** Dönüş: yazıldıysa true. Aynı (sohbet, client_msg_id) zaten varsa false (unique kısıt karar verir). */
    public boolean insert(long conversationId, long seq, long senderId, UUID clientMsgId, String kind, String body,
                          Long paymentRequestId) {
        return jdbc.sql("""
                        INSERT INTO messages (conversation_id, seq, sender_id, client_msg_id, kind, body, payment_request_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (conversation_id, client_msg_id) DO NOTHING""")
                .params(conversationId, seq, senderId, clientMsgId, kind, body, paymentRequestId)
                .update() == 1;
    }

    public Optional<MessageView> byClientId(long conversationId, UUID clientMsgId) {
        return jdbc.sql(SELECT + " WHERE m.conversation_id = ? AND m.client_msg_id = ?")
                .params(conversationId, clientMsgId).query(this::map).optional();
    }

    public Optional<MessageView> bySeq(long conversationId, long seq) {
        return jdbc.sql(SELECT + " WHERE m.conversation_id = ? AND m.seq = ?")
                .params(conversationId, seq).query(this::map).optional();
    }

    /** Senkron: imleçten sonraki mesajlar, eskiden yeniye. */
    public List<MessageView> after(long conversationId, long afterSeq, int limit) {
        return jdbc.sql(SELECT + " WHERE m.conversation_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?")
                .params(conversationId, afterSeq, limit).query(this::map).list();
    }

    /** Geçmiş: verilen seq'ten önceki mesajlar, yeniden eskiye (sayfalı kaydırma). */
    public List<MessageView> before(long conversationId, long beforeSeq, int limit) {
        return jdbc.sql(SELECT + " WHERE m.conversation_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?")
                .params(conversationId, beforeSeq, limit).query(this::map).list();
    }

    private MessageView map(ResultSet rs, int n) throws SQLException {
        Map<String, Object> pr = null;
        long prId = rs.getLong("pr_id");
        if (!rs.wasNull()) {
            pr = new LinkedHashMap<>();
            pr.put("id", prId);
            pr.put("status", rs.getString("pr_status"));
            pr.put("amountKurus", rs.getLong("pr_amount"));
            pr.put("payerId", rs.getLong("pr_payer"));
        }
        return new MessageView(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5),
                rs.getString(6), rs.getTimestamp(7).toInstant().toString(), pr);
    }
}
