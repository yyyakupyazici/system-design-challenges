package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.paramix.provision.domain.Provision.OperationType;
import io.paramix.provision.domain.Provision.Status;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Durum geçişlerinin hepsi koşullu UPDATE ... WHERE status = 'HELD' RETURNING. İki istek aynı provizyonu aynı anda
 * kapatmaya çalışırsa satır kilidi onları sıraya sokar; ikincisi koşulu sağlamaz, 0 satır alır ve hiçbir şey
 * değiştirmez. "Önce oku, durum HELD ise güncelle" yazılsaydı ikisi de HELD görüp ikisi de para hareket ettirirdi.
 */
@Repository
public class ProvisionRepository {

    private static final String COLUMNS = """
            id, customer_id, asset, amount, captured_amount, operation_type, operation_ref, status, expires_at,
            created_at, updated_at""";

    private static final RowMapper<Provision> MAPPER = ProvisionRepository::map;

    private final JdbcTemplate jdbc;

    public ProvisionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return false: aynı (müşteri, işlem tipi, işlem referansı) için zaten bir provizyon var. */
    public boolean insertHeld(UUID id, long customerId, String asset, BigDecimal amount, OperationType type,
                              String operationRef, Instant expiresAt) {
        return jdbc.update("""
                        INSERT INTO provisions (id, customer_id, asset, amount, operation_type, operation_ref, status, expires_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'HELD', ?)
                        ON CONFLICT ON CONSTRAINT uq_provision_operation DO NOTHING
                        """, id, customerId, asset, amount, type.name(), operationRef,
                expiresAt == null ? null : Timestamp.from(expiresAt)) == 1;
    }

    public Optional<Provision> findById(UUID id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM provisions WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public Optional<Provision> findByOperation(long customerId, OperationType type, String operationRef) {
        return jdbc.query("SELECT " + COLUMNS + " FROM provisions WHERE customer_id = ? AND operation_type = ? AND operation_ref = ?",
                MAPPER, customerId, type.name(), operationRef).stream().findFirst();
    }

    public Optional<Provision> capture(UUID id, BigDecimal capturedAmount) {
        return jdbc.query("""
                UPDATE provisions
                SET status = 'CAPTURED', captured_amount = ?, version = version + 1, updated_at = now()
                WHERE id = ? AND status = 'HELD' AND ? <= amount
                RETURNING""" + " " + COLUMNS, MAPPER, capturedAmount, id, capturedAmount).stream().findFirst();
    }

    public Optional<Provision> close(UUID id, Status target) {
        return jdbc.query("""
                UPDATE provisions
                SET status = ?, version = version + 1, updated_at = now()
                WHERE id = ? AND status = 'HELD'
                RETURNING""" + " " + COLUMNS, MAPPER, target.name(), id).stream().findFirst();
    }

    /**
     * Süresi dolan provizyonları EXPIRED yapar. {@code SKIP LOCKED}: o anda capture/release edilmekte olan satırı
     * beklemez, atlar (o satırın sahibi işini bitirsin). Birden çok pod aynı anda süpürse de aynı satırı iki kez almaz.
     */
    public List<Provision> expireDue(int limit) {
        return jdbc.query("""
                WITH due AS (
                    SELECT id FROM provisions
                    WHERE status = 'HELD' AND expires_at IS NOT NULL AND expires_at <= now()
                    ORDER BY expires_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED)
                UPDATE provisions p
                SET status = 'EXPIRED', version = p.version + 1, updated_at = now()
                FROM due WHERE p.id = due.id
                RETURNING p.id, p.customer_id, p.asset, p.amount, p.captured_amount, p.operation_type, p.operation_ref,
                          p.status, p.expires_at, p.created_at, p.updated_at
                """, MAPPER, limit);
    }

    private static Provision map(ResultSet rs, int i) throws SQLException {
        Timestamp expires = rs.getTimestamp("expires_at");
        return new Provision(rs.getObject("id", UUID.class), rs.getLong("customer_id"), rs.getString("asset"),
                rs.getBigDecimal("amount"), rs.getBigDecimal("captured_amount"),
                OperationType.valueOf(rs.getString("operation_type")), rs.getString("operation_ref"),
                Status.valueOf(rs.getString("status")), expires == null ? null : expires.toInstant(),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
    }
}
