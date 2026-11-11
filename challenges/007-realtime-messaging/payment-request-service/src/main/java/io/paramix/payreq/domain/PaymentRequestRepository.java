package io.paramix.payreq.domain;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Bütün durum geçişleri KOŞULLU UPDATE: "şu durumdaysa şuna geç". Aynı geçişi iki yol (senkron cevap, cüzdan olayı,
 * reconciler) aynı anda denese de sadece biri başarılı olur. Her başarılı geçiş sürümü artırır.
 */
@Repository
public class PaymentRequestRepository {

    private static final String COLS = "id, requester_id, payer_id, amount_kurus, description, origin_kind, origin_ref, "
            + "status, version, wallet_transfer_id, reversal_reason, expires_at";

    private final JdbcClient jdbc;

    public PaymentRequestRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Idempotent: aynı (isteyen, anahtar) ile ikinci çağrı yeni satır açmaz. Dönüş boşsa zaten vardı. */
    public Optional<PaymentRequest> create(long requesterId, long payerId, long amountKurus, String description,
                                           String originKind, String originRef, String idempotencyKey, Instant expiresAt) {
        return jdbc.sql("INSERT INTO payment_requests (requester_id, payer_id, amount_kurus, description, origin_kind, "
                        + "origin_ref, idempotency_key, status, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?) "
                        + "ON CONFLICT (requester_id, idempotency_key) DO NOTHING RETURNING " + COLS)
                .params(requesterId, payerId, amountKurus, description, originKind, originRef, idempotencyKey,
                        Timestamp.from(expiresAt))
                .query(this::map).optional();
    }

    public Optional<PaymentRequest> byIdempotencyKey(long requesterId, String key) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE requester_id = ? AND idempotency_key = ?")
                .params(requesterId, key).query(this::map).optional();
    }

    public Optional<PaymentRequest> find(long id) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE id = ?").param(id).query(this::map).optional();
    }

    /** Kapma: PENDING → PROCESSING. İki cihazdan aynı anda "Öde": sadece biri geçer. */
    public Optional<PaymentRequest> claim(long id, long payerId) {
        return transition("status = 'PROCESSING'",
                "id = ? AND payer_id = ? AND status = 'PENDING' AND expires_at > now()", id, payerId);
    }

    public Optional<PaymentRequest> markPaid(long id, String transferId) {
        return transition("status = 'PAID', wallet_transfer_id = ?", "id = ? AND status = 'PROCESSING'", transferId, id);
    }

    /**
     * Cüzdan "tamamlandı" dedi: para gerçekten gittiyse istek hangi durumdaysa ödenmiş sayılır (cüzdan ekranından ya da
     * süre dolduktan sonra ödenmiş olabilir). Tutar WHERE'de: uyuşmayan transfer isteği ödenmiş yapamaz.
     */
    public Optional<PaymentRequest> markPaidByWallet(long id, String transferId, long amountKurus) {
        return transition("status = 'PAID', wallet_transfer_id = ?",
                "id = ? AND amount_kurus = ? AND status IN ('PENDING', 'PROCESSING', 'EXPIRED', 'DECLINED')",
                transferId, id, amountKurus);
    }

    /** Cüzdan reddetti (bakiye yetersiz): tekrar denenebilsin. Olay yayınlanmaz, dışarıdan bakınca hâlâ "bekliyor". */
    public boolean release(long id) {
        return jdbc.sql("UPDATE payment_requests SET status = 'PENDING', updated_at = now() WHERE id = ? AND status = 'PROCESSING'")
                .param(id).update() == 1;
    }

    public Optional<PaymentRequest> decline(long id, long payerId) {
        return transition("status = 'DECLINED'", "id = ? AND payer_id = ? AND status = 'PENDING'", id, payerId);
    }

    public Optional<PaymentRequest> expire(long id) {
        return transition("status = 'EXPIRED'", "id = ? AND status = 'PENDING' AND expires_at <= now()", id);
    }

    public Optional<PaymentRequest> markReversed(String transferId, String reason) {
        return transition("status = 'REVERSED', reversal_reason = ?", "wallet_transfer_id = ? AND status = 'PAID'",
                reason, transferId);
    }

    public List<PaymentRequest> stuckProcessing(long olderThanMs) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE status = 'PROCESSING' "
                        + "AND updated_at < now() - make_interval(secs => ?) ORDER BY id LIMIT 100")
                .param(olderThanMs / 1000.0).query(this::map).list();
    }

    public List<Long> expiredPending() {
        return jdbc.sql("SELECT id FROM payment_requests WHERE status = 'PENDING' AND expires_at <= now() ORDER BY id LIMIT 100")
                .query(Long.class).list();
    }

    public List<PaymentRequest> pendingFor(long payerId) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE payer_id = ? AND status = 'PENDING' ORDER BY id DESC LIMIT 50")
                .param(payerId).query(this::map).list();
    }

    private Optional<PaymentRequest> transition(String set, String where, Object... params) {
        return jdbc.sql("UPDATE payment_requests SET " + set + ", version = version + 1, updated_at = now() WHERE "
                        + where + " RETURNING " + COLS)
                .params(params).query(this::map).optional();
    }

    private PaymentRequest map(ResultSet rs, int n) throws SQLException {
        return new PaymentRequest(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4), rs.getString(5),
                rs.getString(6), rs.getString(7), rs.getString(8), rs.getInt(9), rs.getString(10), rs.getString(11),
                rs.getTimestamp(12).toInstant().toString());
    }
}
