package io.paramix.chat.payment;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * Testler ve lokal demo için cüzdan. Idempotent (aynı anahtar = aynı transfer) ve hata senaryoları enjekte edilebilir.
 * Üretimde 006'daki cüzdan servisine HTTP/gRPC ile giden gerçek istemci bu bean'in yerini alır.
 */
@Component
@ConditionalOnMissingBean(name = "walletClient")
public class FakeWalletClient implements WalletClient {

    public enum Mode {
        OK,
        /** En sinsi durum: para gitti ama cevap kayboldu. */
        TIMEOUT_AFTER_TRANSFER,
        /** Cüzdana hiç ulaşılamadı. */
        UNAVAILABLE,
        INSUFFICIENT_FUNDS
    }

    private final Map<String, String> transfersByKey = new ConcurrentHashMap<>();
    private final Queue<Mode> script = new ConcurrentLinkedQueue<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger moneyMovements = new AtomicInteger();

    @Override
    public TransferResult transfer(String idempotencyKey, long fromUserId, long toUserId, long amountKurus) {
        calls.incrementAndGet();
        Mode mode = script.poll();
        if (mode == Mode.UNAVAILABLE) {
            throw new WalletUnavailableException("cüzdan servisine ulaşılamadı");
        }
        if (mode == Mode.INSUFFICIENT_FUNDS && !transfersByKey.containsKey(idempotencyKey)) {
            return TransferResult.rejected("INSUFFICIENT_FUNDS");
        }
        String id = transfersByKey.computeIfAbsent(idempotencyKey, k -> {
            moneyMovements.incrementAndGet();
            return "trf_" + UUID.randomUUID();
        });
        if (mode == Mode.TIMEOUT_AFTER_TRANSFER) {
            throw new WalletUnavailableException("cevap zaman aşımına uğradı");
        }
        return TransferResult.done(id);
    }

    public void script(Mode... modes) {
        script.addAll(java.util.List.of(modes));
    }

    public int calls() {
        return calls.get();
    }

    /** Gerçekte kaç kez para hareket etti (aynı anahtarla tekrarlar sayılmaz). */
    public int moneyMovements() {
        return moneyMovements.get();
    }

    public boolean hasTransfer(String key) {
        return transfersByKey.containsKey(key);
    }
}
