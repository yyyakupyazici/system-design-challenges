package io.paramix.notify.store;

import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class NotificationRepository {

    private final JdbcClient jdbc;

    public NotificationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** ① UNIQUE(idempotency_key): ikinci istek hiçbir şey eklemez, boş döner. */
    public boolean insertIfAbsent(String notificationId, String idempotencyKey, String type, String priority,
                                  String userId, String campaignId, String payloadJson) {
        return jdbc.sql("""
                        INSERT INTO notification (notification_id, idempotency_key, type, priority, user_id, campaign_id, payload)
                        VALUES (:id, :key, :type, :priority, :uid, :cid, CAST(:payload AS jsonb))
                        ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("id", notificationId)
                .param("key", idempotencyKey)
                .param("type", type)
                .param("priority", priority)
                .param("uid", userId)
                .param("cid", campaignId)
                .param("payload", payloadJson)
                .update() == 1;
    }

    public Optional<String> findIdByKey(String idempotencyKey) {
        return jdbc.sql("SELECT notification_id FROM notification WHERE idempotency_key = :key")
                .param("key", idempotencyKey).query(String.class).optional();
    }

    public Optional<Map<String, Object>> find(String notificationId) {
        return jdbc.sql("""
                        SELECT notification_id, idempotency_key, type, priority, user_id, campaign_id, created_at
                        FROM notification WHERE notification_id = :id""")
                .param("id", notificationId).query().listOfRows().stream().findFirst();
    }
}
