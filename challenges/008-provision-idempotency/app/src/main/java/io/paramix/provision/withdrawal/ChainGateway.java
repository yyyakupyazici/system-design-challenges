package io.paramix.provision.withdrawal;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Zincir (ya da banka) tarafı. Gerçek uygulamada imzalama HSM/MPC'de, yayın node'a, durum sorgusu indexer'a gider.
 *
 * <p>Sözleşme, çekimin güvenliğinin temeli:
 * <ul>
 *   <li>{@link #sign} imzalı işlemi ve hash'ini verir. Hash yayından ÖNCE veritabanına yazılır.</li>
 *   <li>{@link #broadcast} aynı imzalı işlem için tekrar çağrılabilir: zincir aynı işlemi (aynı nonce/hash) ikinci
 *       kez kabul etmez. Yani "emin değilsen aynı baytları tekrar yolla" güvenlidir; "yeniden imzala" DEĞİLDİR.</li>
 *   <li>{@link #status} hash ile sorulur: worker çöküp yeniden başladığında "gönderdim mi?" sorusunun cevabı budur.</li>
 * </ul>
 */
public interface ChainGateway {

    record TransferInstruction(UUID withdrawalId, String asset, BigDecimal amount, String destination) {
    }

    record SignedTransaction(String txHash, String payload) {
    }

    enum TxStatus {
        /** Zincir/mempool bu hash'i hiç görmemiş. */
        NOT_FOUND,
        /** Görüldü, onay bekliyor. */
        PENDING,
        /** Yeterli onayı aldı, geri dönmez. */
        CONFIRMED,
        /** Kesin başarısız (revert, geçersiz adres...). Para çıkmadı. */
        FAILED
    }

    SignedTransaction sign(TransferInstruction instruction);

    void broadcast(SignedTransaction tx);

    TxStatus status(String txHash);
}
