package io.paramix.chat.paymentcard;

import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentCardRepository {

    public enum Change { CREATED, UPDATED, STALE }

    private final JdbcClient jdbc;

    public PaymentCardRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Kartı olaydaki anlık görüntüyle yazar, ama sadece olay daha YENİYSE. Tek SQL, yarışsız:
     * <pre>
     *   kart yok              → INSERT            → CREATED
     *   kart var, sürüm eski  → UPDATE            → UPDATED
     *   kart var, sürüm ≥     → WHERE tutmaz       → STALE (tekrar ya da sırası karışmış olay: hiçbir şey yapma)
     * </pre>
     * {@code xmax = 0}: PostgreSQL'de satır bu komutla yeni eklendiyse 0'dır (güncellendiyse değildir).
     */
    public Change upsert(PaymentRequestEvent e, long conversationId) {
        Optional<Boolean> inserted = jdbc.sql("""
                        INSERT INTO payment_cards (payment_request_id, conversation_id, requester_id, payer_id, amount_kurus,
                                                   description, status, version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (payment_request_id) DO UPDATE
                            SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()
                            WHERE payment_cards.version < EXCLUDED.version
                        RETURNING (xmax = 0)""")
                .params(e.paymentRequestId(), conversationId, e.requesterId(), e.payerId(), e.amountKurus(),
                        e.description() == null ? "" : e.description(), e.status(), e.version())
                .query(Boolean.class).optional();
        return inserted.map(i -> i ? Change.CREATED : Change.UPDATED).orElse(Change.STALE);
    }

    public boolean exists(long paymentRequestId) {
        return jdbc.sql("SELECT count(*) FROM payment_cards WHERE payment_request_id = ?").param(paymentRequestId)
                .query(Long.class).single() > 0;
    }
}
