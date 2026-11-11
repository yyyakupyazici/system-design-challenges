package io.paramix.provision.idempotency;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class IdempotencyRepository {

    /** Bir commit edilmiş kaydın tekrar oynatılması için gereken her şey. */
    public record StoredRequest(String operation, byte[] requestHash, int responseStatus, String responseBody,
                                UUID resourceId) {
    }

    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcTemplate jdbc;

    public IdempotencyRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Key'i bu transaction adına "kapmaya" çalışır.
     *
     * <p>Aynı key ile eşzamanlı iki istek geldiğinde PostgreSQL ikincisinin INSERT'ünü, unique indeksteki commit
     * edilmemiş satır yüzünden BEKLETİR. İlki commit ederse ikincisi çakışmayı görür ve {@code DO NOTHING} ile
     * boş döner; ilki rollback olursa (çöktü, hata aldı) ikincisinin INSERT'ü başarılı olur ve işi o yapar.
     * Yani kilit için ayrı bir mekanizmaya (Redis lock, IN_PROGRESS + lease) gerek yok: kilidi veritabanı tutuyor
     * ve çöken sahibin kilidi transaction'la birlikte kendiliğinden kalkıyor.
     *
     * <p>Bekleme süresi {@code lock_timeout} ile sınırlı; aşılırsa istemciye 409 + Retry-After döner ve bağlantı
     * havuzu bekleyen isteklerle tükenmez. Ayar {@code SET LOCAL} ile sadece bu INSERT için verilip hemen geri alınır;
     * böylece işin geri kalanındaki (bakiye satırı vb.) kilit beklemeleri yanlışlıkla "istek hâlâ işleniyor"
     * diye raporlanmaz.
     *
     * @return true: key bu transaction'ın, işi yap. false: key'in commit edilmiş bir kaydı var, onu oku.
     */
    public boolean tryClaim(long customerId, IdempotencyKey key, String operation, byte[] requestHash,
                            long lockWaitMillis) {
        jdbc.queryForObject("SELECT set_config('lock_timeout', ?, true)", String.class, lockWaitMillis + "ms");
        int inserted;
        try {
            inserted = jdbc.update("""
                    INSERT INTO idempotency_keys (key_day, customer_id, idem_key, operation, request_hash)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """, Date.valueOf(key.partitionDay()), customerId, key.value(), operation, requestHash);
        } catch (DataAccessException e) {
            if (!isLockTimeout(e)) {
                throw e;
            }
            throw new IdempotencyException(IdempotencyException.Reason.REQUEST_IN_PROGRESS,
                    "Aynı key ile gelen ilk istek henüz bitmedi; biraz sonra aynı key ile tekrar deneyin");
        }
        jdbc.execute("SET LOCAL lock_timeout TO DEFAULT");
        return inserted == 1;
    }

    /**
     * SQLSTATE 55P03 (lock_not_available). Spring'in hata kodu tablosu bunu her sürümde aynı istisna sınıfına
     * çevirmiyor (UncategorizedSQLException gelebiliyor); sınıfa değil, PostgreSQL'in kendi koduna bakıyoruz.
     */
    private static boolean isLockTimeout(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && LOCK_NOT_AVAILABLE.equals(sql.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    public Optional<StoredRequest> find(long customerId, IdempotencyKey key) {
        return jdbc.query("""
                        SELECT operation, request_hash, response_status, response_body, resource_id
                        FROM idempotency_keys
                        WHERE customer_id = ? AND idem_key = ? AND key_day = ?
                        """,
                (rs, i) -> new StoredRequest(rs.getString(1), rs.getBytes(2), rs.getInt(3), rs.getString(4),
                        rs.getObject(5, UUID.class)),
                customerId, key.value(), Date.valueOf(key.partitionDay())).stream().findFirst();
    }

    public void complete(long customerId, IdempotencyKey key, int status, String body, UUID resourceId) {
        int updated = jdbc.update("""
                UPDATE idempotency_keys
                SET response_status = ?, response_body = ?, resource_id = ?, completed_at = now()
                WHERE customer_id = ? AND idem_key = ? AND key_day = ? AND response_status IS NULL
                """, status, body, resourceId, customerId, key.value(), Date.valueOf(key.partitionDay()));
        if (updated != 1) {
            throw new IllegalStateException("Idempotency kaydı bu transaction'da kapılmamış: " + key.value());
        }
    }
}
