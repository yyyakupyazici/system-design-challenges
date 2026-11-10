package io.paramix.chat.payment;

import io.paramix.chat.message.ChatException;
import io.paramix.chat.payment.PaymentRepository.PaymentRequest;
import io.paramix.chat.payment.WalletClient.TransferResult;
import io.paramix.chat.payment.WalletClient.WalletUnavailableException;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Sohbetteki "Öde" butonu. Mesaj servisi ile cüzdan AYRI sistemler; aralarında dağıtık transaction yok.
 * Tutarlılık üç parçadan gelir:
 *
 * <pre>
 *  ① KAPMA     PENDING → PROCESSING (koşullu UPDATE): iki cihazdan aynı anda basılsa da sadece biri geçer
 *  ② CÜZDAN    transfer(idempotencyKey = "payreq-&lt;id&gt;")  → aynı anahtarla tekrar = aynı transfer, para bir kez
 *  ③ KAPANIŞ   PROCESSING → PAID + sohbete sistem mesajı, TEK transaction'da
 *
 *  ② belirsiz biterse (zaman aşımı: para gitti mi bilinmiyor) istek PROCESSING'de kalır; reconciler aynı anahtarla
 *  tekrar sorar. Cüzdan idempotent olduğu için ya mevcut transferi döner ya da şimdi yapar: iki kez ödeme olamaz.
 * </pre>
 */
@Service
public class PaymentRequestService {

    private static final java.util.Locale TR = java.util.Locale.forLanguageTag("tr-TR");

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestService.class);

    private final PaymentRepository repo;
    private final WalletClient wallet;
    private final SystemMessages system;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;

    public PaymentRequestService(PaymentRepository repo, WalletClient wallet, SystemMessages system,
                                 TransactionTemplate tx, MeterRegistry meters) {
        this.repo = repo;
        this.wallet = wallet;
        this.system = system;
        this.tx = tx;
        this.meters = meters;
    }

    static String walletKey(long paymentRequestId) {
        return "payreq-" + paymentRequestId;
    }

    /** Dönüş: PAID, PROCESSING (sonuç bekleniyor), ALREADY_PAID, EXPIRED, DECLINED, FAILED:&lt;sebep&gt; */
    public String pay(long userId, long paymentRequestId) {
        PaymentRequest pr = repo.find(paymentRequestId).orElseThrow(() -> new ChatException("NOT_FOUND", "para isteği yok"));
        if (pr.payerId() != userId) {
            throw new ChatException("FORBIDDEN", "bu isteği sadece istenen kişi ödeyebilir");
        }
        return repo.claim(paymentRequestId, userId).map(this::complete).orElseGet(() -> {
            PaymentRequest now = repo.find(paymentRequestId).orElseThrow();
            meters.counter("chat.payment.claim_lost", "status", now.status()).increment();
            return switch (now.status()) {
                case "PAID" -> "ALREADY_PAID";
                case "PENDING" -> "EXPIRED";            // kapma başarısız ama PENDING: süresi dolmuş, reaper henüz kapatmamış
                default -> now.status();
            };
        });
    }

    /** ② ve ③. Hem ilk ödemede hem reconciler'da aynı yol: cüzdan idempotent olduğu için tekrar güvenli. */
    String complete(PaymentRequest pr) {
        TransferResult r;
        try {
            r = wallet.transfer(walletKey(pr.id()), pr.payerId(), pr.requesterId(), pr.amountKurus());
        } catch (WalletUnavailableException e) {
            log.warn("Para isteği {} için cüzdan cevabı belirsiz, reconciler tekrar deneyecek: {}", pr.id(), e.getMessage());
            meters.counter("chat.payment.uncertain").increment();
            return "PROCESSING";
        }
        if (!r.ok()) {
            repo.release(pr.id());
            return "FAILED:" + r.rejectReason();
        }
        tx.executeWithoutResult(s -> {
            if (repo.markPaid(pr.id(), r.transferId())) {
                system.append(pr.conversationId(), pr.id(), "PAID",
                        String.format(TR, "✅ %,.2f TL ödendi", pr.amountKurus() / 100.0), "Para isteğin ödendi");
            }
        });
        meters.counter("chat.payment.paid").increment();
        return "PAID";
    }

    public boolean decline(long userId, long paymentRequestId) {
        PaymentRequest pr = repo.find(paymentRequestId).orElseThrow(() -> new ChatException("NOT_FOUND", "para isteği yok"));
        Boolean done = tx.execute(s -> {
            if (!repo.decline(paymentRequestId, userId)) {
                return false;
            }
            system.append(pr.conversationId(), pr.id(), "DECLINED", "❌ Para isteği reddedildi", "Para isteğin reddedildi");
            return true;
        });
        return Boolean.TRUE.equals(done);
    }

    // --- reconciler ve süre dolumu (PaymentJobs çağırır) ---

    void expire(long id) {
        PaymentRequest pr = repo.find(id).orElseThrow();
        tx.executeWithoutResult(s -> {
            if (repo.markExpired(id)) {
                system.append(pr.conversationId(), id, "EXPIRED", "⌛ Para isteğinin süresi doldu", null);
            }
        });
    }
}
