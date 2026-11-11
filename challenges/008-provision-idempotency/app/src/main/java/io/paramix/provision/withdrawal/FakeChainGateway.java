package io.paramix.provision.withdrawal;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.provision.config.ProvisionProperties;

import org.springframework.stereotype.Component;

/**
 * Bellek içi zincir. Gerçek bir zincirin çekim güvenliği açısından önemli davranışlarını taklit eder:
 * <ul>
 *   <li>İmza deterministik: aynı talimat → aynı hash.</li>
 *   <li>Aynı işlem ikinci kez yayınlanırsa yeni transfer OLUŞMAZ (nonce koruması).</li>
 *   <li>Testler için arıza enjeksiyonu: "yayından hemen sonra süreç ölsün".</li>
 * </ul>
 */
@Component
public class FakeChainGateway implements ChainGateway {

    /** Süreç ölümünü taklit eder. Error olduğu için worker'ın normal hata yakalaması onu tutmaz: iş yarıda kalır. */
    public static final class SimulatedCrash extends Error {
        public SimulatedCrash(String message) {
            super(message);
        }
    }

    private static final class Tx {
        volatile TxStatus status = TxStatus.PENDING;
        final AtomicInteger polls = new AtomicInteger();
    }

    private final Map<String, Tx> mempool = new ConcurrentHashMap<>();
    private final AtomicInteger broadcastCalls = new AtomicInteger();
    private final AtomicBoolean crashAfterNextBroadcast = new AtomicBoolean();
    private final AtomicInteger unavailableCalls = new AtomicInteger();
    private final int confirmAfterPolls;

    public FakeChainGateway(ProvisionProperties props) {
        this.confirmAfterPolls = props.chain().fakeConfirmAfterPolls();
    }

    @Override
    public SignedTransaction sign(TransferInstruction in) {
        String payload = in.withdrawalId() + "|" + in.asset() + "|" + in.amount().stripTrailingZeros().toPlainString()
                + "|" + in.destination();
        return new SignedTransaction("0x" + sha256(payload), payload);
    }

    @Override
    public void broadcast(SignedTransaction tx) {
        failIfUnavailable();
        broadcastCalls.incrementAndGet();
        if (!tx.txHash().equals("0x" + sha256(tx.payload()))) {
            throw new IllegalArgumentException("İmza ile hash uyuşmuyor");
        }
        mempool.putIfAbsent(tx.txHash(), new Tx());   // aynı hash ikinci kez yeni transfer yaratmaz
        if (crashAfterNextBroadcast.compareAndSet(true, false)) {
            throw new SimulatedCrash("Süreç yayından hemen sonra, sonucu yazamadan öldü");
        }
    }

    @Override
    public TxStatus status(String txHash) {
        failIfUnavailable();
        Tx tx = mempool.get(txHash);
        if (tx == null) {
            return TxStatus.NOT_FOUND;
        }
        if (tx.status == TxStatus.PENDING && confirmAfterPolls > 0 && tx.polls.incrementAndGet() >= confirmAfterPolls) {
            tx.status = TxStatus.CONFIRMED;
        }
        return tx.status;
    }

    // ---- test ve demo kontrolleri ----

    public void crashAfterNextBroadcast() {
        crashAfterNextBroadcast.set(true);
    }

    /** Sonraki {@code n} çağrı "node erişilemiyor" hatası versin. */
    public void unavailableFor(int n) {
        unavailableCalls.set(n);
    }

    public void confirm(String txHash) {
        mempool.get(txHash).status = TxStatus.CONFIRMED;
    }

    public void fail(String txHash) {
        mempool.get(txHash).status = TxStatus.FAILED;
    }

    public int broadcastCalls() {
        return broadcastCalls.get();
    }

    /** Zincirde gerçekten oluşmuş transfer sayısı. Çift gönderim olsaydı bu, çekim sayısından büyük olurdu. */
    public int distinctTransfers() {
        return mempool.size();
    }

    public void reset() {
        mempool.clear();
        broadcastCalls.set(0);
        crashAfterNextBroadcast.set(false);
        unavailableCalls.set(0);
    }

    private void failIfUnavailable() {
        if (unavailableCalls.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new IllegalStateException("Zincir node'u erişilemiyor");
        }
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
