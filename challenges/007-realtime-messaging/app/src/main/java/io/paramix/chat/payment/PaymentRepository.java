package io.paramix.chat.payment;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    public record PaymentRequest(long id, long conversationId, long requesterId, long payerId, long amountKurus,
                                 String status, String walletTransferId) {
    }

    private static final String COLS = "id, conversation_id, requester_id, payer_id, amount_kurus, status, wallet_transfer_id";

    private final JdbcClient jdbc;

    public PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long create(long conversationId, long requesterId, long payerId, long amountKurus, Instant expiresAt) {
        return jdbc.sql("""
                        INSERT INTO payment_requests (conversation_id, requester_id, payer_id, amount_kurus, status, expires_at)
                        VALUES (?, ?, ?, ?, 'PENDING', ?) RETURNING id""")
                .params(conversationId, requesterId, payerId, amountKurus, Timestamp.from(expiresAt))
                .query(Long.class).single();
    }

    public Optional<PaymentRequest> find(long id) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE id = ?").param(id).query(this::map).optional();
    }

    /**
     * Ödeme hakkını KAPAR: PENDING → PROCESSING, sadece biri başarılı olur. İki cihazdan aynı anda "Öde"ye
     * basılsa da cüzdana tek bir çağrı gider. (Redis Lab'daki "ilk gelen alır" kalıbının veritabanı hali.)
     */
    public Optional<PaymentRequest> claim(long id, long payerId) {
        return jdbc.sql("UPDATE payment_requests SET status = 'PROCESSING', updated_at = now() "
                        + "WHERE id = ? AND payer_id = ? AND status = 'PENDING' AND expires_at > now() RETURNING " + COLS)
                .params(id, payerId).query(this::map).optional();
    }

    public boolean markPaid(long id, String transferId) {
        return jdbc.sql("UPDATE payment_requests SET status = 'PAID', wallet_transfer_id = ?, updated_at = now() "
                        + "WHERE id = ? AND status = 'PROCESSING'")
                .params(transferId, id).update() == 1;
    }

    /** Cüzdan kesin olarak reddetti (bakiye yetersiz): tekrar denenebilsin diye PENDING'e döner. */
    public boolean release(long id) {
        return jdbc.sql("UPDATE payment_requests SET status = 'PENDING', updated_at = now() WHERE id = ? AND status = 'PROCESSING'")
                .param(id).update() == 1;
    }

    /**
     * Cüzdan "transfer tamamlandı" dedi. Para gerçekten hareket ettiyse sohbet bunu yansıtmalı, istek hangi
     * durumdaysa (sohbetten değil cüzdan ekranından ödenmiş, ya da süresi dolduktan sonra ödenmiş olabilir).
     * Tutar uyuşmuyorsa hiçbir şey yapılmaz (çağıran ayrıca kontrol edip alarm üretir). Zaten PAID ise etkisiz.
     */
    public boolean markPaidByWallet(long id, String transferId, long amountKurus) {
        return jdbc.sql("UPDATE payment_requests SET status = 'PAID', wallet_transfer_id = ?, updated_at = now() "
                        + "WHERE id = ? AND amount_kurus = ? AND status IN ('PENDING', 'PROCESSING', 'EXPIRED', 'DECLINED')")
                .params(transferId, id, amountKurus).update() == 1;
    }

    /** Ödeme iade edildi. Sadece PAID → REVERSED; aynı olay ikinci kez gelirse etkisiz. */
    public Optional<PaymentRequest> markReversed(String transferId, String reason) {
        return jdbc.sql("UPDATE payment_requests SET status = 'REVERSED', reversal_reason = ?, updated_at = now() "
                        + "WHERE wallet_transfer_id = ? AND status = 'PAID' RETURNING " + COLS)
                .params(reason, transferId).query(this::map).optional();
    }

    public boolean markExpired(long id) {
        return jdbc.sql("UPDATE payment_requests SET status = 'EXPIRED', updated_at = now() WHERE id = ? AND status = 'PENDING'")
                .param(id).update() == 1;
    }

    public boolean decline(long id, long payerId) {
        return jdbc.sql("UPDATE payment_requests SET status = 'DECLINED', updated_at = now() "
                        + "WHERE id = ? AND payer_id = ? AND status = 'PENDING'")
                .params(id, payerId).update() == 1;
    }

    /** PROCESSING'de takılanlar: cüzdan cevabı kayboldu ya da pod öldü. Aynı anahtarla tekrar sorulacak. */
    public List<PaymentRequest> stuckProcessing(long olderThanMs) {
        return jdbc.sql("SELECT " + COLS + " FROM payment_requests WHERE status = 'PROCESSING' "
                        + "AND updated_at < now() - make_interval(secs => ?) ORDER BY id LIMIT 100")
                .param(olderThanMs / 1000.0).query(this::map).list();
    }

    public List<Long> expiredPending() {
        return jdbc.sql("SELECT id FROM payment_requests WHERE status = 'PENDING' AND expires_at <= now() ORDER BY id LIMIT 100")
                .query(Long.class).list();
    }

    private PaymentRequest map(java.sql.ResultSet rs, int n) throws java.sql.SQLException {
        return new PaymentRequest(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4), rs.getLong(5),
                rs.getString(6), rs.getString(7));
    }
}
