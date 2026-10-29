package io.paramix.notify.store;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    public record OutboxRow(long id, String topic, String key, String payload) {
    }

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(String topic, String key, String payloadJson) {
        jdbc.sql("INSERT INTO outbox (topic, msg_key, payload) VALUES (:t, :k, CAST(:p AS jsonb))")
                .param("t", topic).param("k", key).param("p", payloadJson).update();
    }

    /** SKIP LOCKED: birden fazla scheduler pod'u aynı satırları kapışmadan paralel çalışır. Transaction içinde çağrılmalı. */
    public List<OutboxRow> lockBatch(int limit) {
        return jdbc.sql("""
                        SELECT id, topic, msg_key, payload::text AS payload FROM outbox
                        WHERE published_at IS NULL ORDER BY id LIMIT :limit
                        FOR UPDATE SKIP LOCKED""")
                .param("limit", limit)
                .query((rs, i) -> new OutboxRow(rs.getLong("id"), rs.getString("topic"),
                        rs.getString("msg_key"), rs.getString("payload")))
                .list();
    }

    public void markPublished(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)").param("ids", ids).update();
    }
}
