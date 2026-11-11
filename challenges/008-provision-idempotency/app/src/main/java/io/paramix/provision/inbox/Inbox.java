package io.paramix.provision.inbox;

import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tüketici tarafında "bu mesajı daha önce işledim mi?" kaydı. API idempotency'sinin birebir aynası:
 * aynı mesaj iki tüketiciye aynı anda düşerse (rebalance sırasında olur) ikincisi PRIMARY KEY'de bekler,
 * ilki commit edince çakışmayı görüp atlar; ilki ölürse (rollback) işi ikincisi yapar.
 */
@Repository
public class Inbox {

    private final JdbcTemplate jdbc;

    public Inbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true: mesaj ilk kez görülüyor ve bu transaction'ın; false: daha önce işlenmiş, atla. */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean tryRecord(String consumer, UUID messageId, String messageType) {
        return jdbc.update("""
                INSERT INTO inbox (consumer, message_id, message_type, outcome)
                VALUES (?, ?, ?, 'APPLIED')
                ON CONFLICT DO NOTHING
                """, consumer, messageId, messageType) == 1;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markConflict(String consumer, UUID messageId, String detail) {
        jdbc.update("UPDATE inbox SET outcome = 'CONFLICT', detail = left(?, 500) WHERE consumer = ? AND message_id = ?",
                detail, consumer, messageId);
    }
}
