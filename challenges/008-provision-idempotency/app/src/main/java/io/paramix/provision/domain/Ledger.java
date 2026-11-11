package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.paramix.provision.support.UuidV7;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Çift kayıtlı, sadece eklenen (append-only) defter. Her bakiye değişikliği burada bir journal olarak iz bırakır.
 * Journal'ın satırları toplamı sıfır değilse yazılmaz: para yoktan var olamaz, yok olamaz.
 */
@Component
public class Ledger {

    public enum Bucket { AVAILABLE, HELD, EXTERNAL, SETTLED }

    public enum EntryType { DEPOSIT, HOLD, CAPTURE, RELEASE, EXPIRE }

    private record Line(Bucket bucket, BigDecimal delta) {
    }

    public final class Journal {
        private final long customerId;
        private final String asset;
        private final EntryType type;
        private final UUID refId;
        private final List<Line> lines = new ArrayList<>();

        private Journal(long customerId, String asset, EntryType type, UUID refId) {
            this.customerId = customerId;
            this.asset = asset;
            this.type = type;
            this.refId = refId;
        }

        /** {@code amount} kadar parayı {@code from} kovasından {@code to} kovasına taşır. */
        public Journal move(Bucket from, Bucket to, BigDecimal amount) {
            if (amount.signum() > 0) {
                lines.add(new Line(from, amount.negate()));
                lines.add(new Line(to, amount));
            }
            return this;
        }

        public void post() {
            BigDecimal sum = lines.stream().map(Line::delta).reduce(BigDecimal.ZERO, BigDecimal::add);
            if (sum.signum() != 0 || lines.isEmpty()) {
                throw new IllegalStateException("Dengesiz journal: " + type + " " + refId + " toplam=" + sum);
            }
            UUID journalId = UuidV7.generate(clock);
            jdbc.batchUpdate("""
                            INSERT INTO ledger_entries (journal_id, customer_id, asset, bucket, delta, entry_type, ref_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, lines, lines.size(),
                    (ps, line) -> {
                        ps.setObject(1, journalId);
                        ps.setLong(2, customerId);
                        ps.setString(3, asset);
                        ps.setString(4, line.bucket().name());
                        ps.setBigDecimal(5, line.delta());
                        ps.setString(6, type.name());
                        ps.setObject(7, refId);
                    });
        }
    }

    private final JdbcTemplate jdbc;
    private final Clock clock;

    public Ledger(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    public Journal journal(long customerId, String asset, EntryType type, UUID refId) {
        return new Journal(customerId, asset, type, refId);
    }
}
