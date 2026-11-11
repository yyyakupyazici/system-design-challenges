package io.paramix.provision.withdrawal;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Çekim satırı ve worker kirası (lease).
 *
 * <p>Kira = (claim_token, lease_until). Her yazma {@code WHERE claim_token = :benim} koşuluyla yapılır (fencing):
 * kirası devralınmış eski worker 0 satır günceller ve {@link LostLeaseException} ile çekilir. Kira süresinin
 * dolması tek başına eski worker'ı durdurmaz; sadece BAŞKA bir worker devraldığında token değişir. Bu yüzden
 * süre dolmuş ama devralınmamış kira ile işe devam etmek güvenlidir.
 *
 * <p>Zaman her yerde veritabanının {@code now()}'ı: pod saatleri kayabilir, kira kararı tek bir saate göre verilmeli.
 */
@Repository
public class WithdrawalRepository {

    private static final String COLUMNS = """
            id, customer_id, asset, amount, destination, provision_id, status, tx_hash, signed_payload, claim_token,
            failures, last_error, created_at, updated_at""";

    private static final RowMapper<Withdrawal> MAPPER = WithdrawalRepository::map;

    private final JdbcTemplate jdbc;

    public WithdrawalRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID id, long customerId, String asset, BigDecimal amount, String destination, UUID provisionId) {
        jdbc.update("""
                INSERT INTO withdrawals (id, customer_id, asset, amount, destination, provision_id, status)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                """, id, customerId, asset, amount, destination, provisionId);
    }

    public Optional<Withdrawal> findById(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM withdrawals WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    /**
     * Sıradaki işi kiralar. {@code FOR UPDATE SKIP LOCKED}: N worker aynı anda çağırsa da her biri farklı satır alır,
     * kimse kimseyi beklemez. Kirası dolmuş satır (sahibi öldü ya da takıldı) yeniden kiralanabilir; yeni token
     * eski sahibin yazmalarını geçersiz kılar.
     */
    public Optional<Withdrawal> claimNext(UUID token, Duration lease) {
        return jdbc.query("""
                UPDATE withdrawals
                SET claim_token = ?, lease_until = now() + make_interval(secs => ?), updated_at = now()
                WHERE id = (
                    SELECT id FROM withdrawals
                    WHERE status IN ('PENDING', 'SIGNED', 'BROADCAST')
                      AND next_attempt_at <= now()
                      AND (lease_until IS NULL OR lease_until < now())
                    ORDER BY next_attempt_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED)
                RETURNING""" + " " + COLUMNS, MAPPER, token, lease.toMillis() / 1000.0).stream().findFirst();
    }

    /** İmzalı işlem YAYINDAN ÖNCE kalıcı olur. Çökme sonrası devralan worker hash ile zincire sorar. */
    public boolean markSigned(UUID id, UUID token, String txHash, String signedPayload) {
        return jdbc.update("""
                UPDATE withdrawals SET status = 'SIGNED', tx_hash = ?, signed_payload = ?, failures = 0, updated_at = now()
                WHERE id = ? AND claim_token = ? AND status = 'PENDING'
                """, txHash, signedPayload, id, token) == 1;
    }

    public boolean markBroadcast(UUID id, UUID token) {
        return jdbc.update("""
                UPDATE withdrawals SET status = 'BROADCAST', failures = 0, updated_at = now()
                WHERE id = ? AND claim_token = ? AND status = 'SIGNED'
                """, id, token) == 1;
    }

    public boolean markFinal(UUID id, UUID token, Withdrawal.Status status) {
        return jdbc.update("""
                UPDATE withdrawals
                SET status = ?, claim_token = NULL, lease_until = NULL, updated_at = now()
                WHERE id = ? AND claim_token = ? AND status = 'BROADCAST'
                """, status.name(), id, token) == 1;
    }

    /** Kirayı bırakır ve işi ileri bir zamana planlar (ör. onay bekleniyor). */
    public boolean reschedule(UUID id, UUID token, Duration delay) {
        return jdbc.update("""
                UPDATE withdrawals
                SET claim_token = NULL, lease_until = NULL, next_attempt_at = now() + make_interval(secs => ?), updated_at = now()
                WHERE id = ? AND claim_token = ?
                """, delay.toMillis() / 1000.0, id, token) == 1;
    }

    /**
     * Hata sayacı. Üst üste {@code maxFailures} hatadan sonra MANUAL_REVIEW: provizyon BLOKE KALIR. Para zincire
     * gitmiş olabilirken otomatik iade, müşteriye bedava para demektir; emin olunamayan durumu insan çözer.
     */
    public boolean recordFailure(UUID id, UUID token, String error, Duration backoff, int maxFailures) {
        return jdbc.update("""
                UPDATE withdrawals
                SET failures = failures + 1,
                    last_error = left(?, 500),
                    status = CASE WHEN failures + 1 >= ? THEN 'MANUAL_REVIEW' ELSE status END,
                    claim_token = NULL, lease_until = NULL,
                    next_attempt_at = now() + make_interval(secs => ?),
                    updated_at = now()
                WHERE id = ? AND claim_token = ?
                """, error, maxFailures, backoff.toMillis() / 1000.0, id, token) == 1;
    }

    private static Withdrawal map(ResultSet rs, int i) throws SQLException {
        return new Withdrawal(rs.getObject("id", UUID.class), rs.getLong("customer_id"), rs.getString("asset"),
                rs.getBigDecimal("amount"), rs.getString("destination"), rs.getObject("provision_id", UUID.class),
                Withdrawal.Status.valueOf(rs.getString("status")), rs.getString("tx_hash"),
                rs.getString("signed_payload"), rs.getObject("claim_token", UUID.class), rs.getInt("failures"),
                rs.getString("last_error"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
