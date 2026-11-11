package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Bakiye hareketleri. Hepsi TEK atomik UPDATE: önce okuyup sonra yazmak (read-modify-write) iki eşzamanlı
 * provizyonun aynı parayı iki kez bloke etmesine yol açar. Koşul WHERE'de olduğu için kontrol ve düşüm bölünmez;
 * satır kilidi eşzamanlı istekleri sıraya sokar, CHECK (available >= 0) da son savunma hattıdır.
 */
@Repository
public class BalanceRepository {

    public record Balance(long customerId, String asset, BigDecimal available, BigDecimal held) {
    }

    private final JdbcTemplate jdbc;

    public BalanceRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** available → held. Yetersizse hiçbir şey değişmez ve false döner. */
    public boolean hold(long customerId, String asset, BigDecimal amount) {
        return jdbc.update("""
                UPDATE balances
                SET available = available - ?, held = held + ?, version = version + 1, updated_at = now()
                WHERE customer_id = ? AND asset = ? AND available >= ?
                """, amount, amount, customerId, asset, amount) == 1;
    }

    /**
     * held'den {@code total} düşer; bunun {@code returned} kadarı available'a döner, kalanı hesaptan çıkar (capture).
     * Provizyon HELD iken held en az o kadardır; tutmuyorsa bir değişmez (invariant) bozulmuştur, sessizce geçilmez.
     */
    public void settleHeld(long customerId, String asset, BigDecimal total, BigDecimal returned) {
        int updated = jdbc.update("""
                UPDATE balances
                SET held = held - ?, available = available + ?, version = version + 1, updated_at = now()
                WHERE customer_id = ? AND asset = ? AND held >= ?
                """, total, returned, customerId, asset, total);
        if (updated != 1) {
            throw new IllegalStateException("Değişmez bozuldu: held < provizyon (" + customerId + "/" + asset + ")");
        }
    }

    public void credit(long customerId, String asset, BigDecimal amount) {
        jdbc.update("""
                INSERT INTO balances (customer_id, asset, available) VALUES (?, ?, ?)
                ON CONFLICT (customer_id, asset)
                DO UPDATE SET available = balances.available + EXCLUDED.available,
                              version = balances.version + 1, updated_at = now()
                """, customerId, asset, amount);
    }

    public List<Balance> findByCustomer(long customerId) {
        return jdbc.query("SELECT customer_id, asset, available, held FROM balances WHERE customer_id = ? ORDER BY asset",
                (rs, i) -> new Balance(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3), rs.getBigDecimal(4)),
                customerId);
    }
}
