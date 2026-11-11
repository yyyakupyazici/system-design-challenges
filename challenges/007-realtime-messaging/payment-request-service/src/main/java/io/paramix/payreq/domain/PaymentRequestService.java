package io.paramix.payreq.domain;

import java.time.Instant;
import java.util.Optional;

import io.paramix.payreq.config.PayReqProperties;
import io.paramix.payreq.outbox.Outbox;
import io.paramix.payreq.wallet.WalletClient;
import io.paramix.payreq.wallet.WalletClient.TransferResult;
import io.paramix.payreq.wallet.WalletClient.WalletUnavailableException;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Para isteğinin bütün kuralları burada, tek yerde. Sohbet, ödeme linki ve fatura aynı servisi kullanır.
 *
 * <pre>
 *  OLUŞTUR   PENDING + CREATED olayı (tek tx). Idempotency key ile: "iste"ye iki kez basılsa da tek istek.
 *  ÖDE       ① KAPMA     PENDING → PROCESSING (koşullu UPDATE, iki cihazdan basılsa da biri)
 *            ② CÜZDAN    transfer(key = "payreq-&lt;id&gt;")  aynı anahtarla tekrar = aynı transfer
 *            ③ KAPANIŞ   PROCESSING → PAID + PAID olayı (tek tx)
 *
 *  Üç yol aynı geçişi dener, hangisi önce gelirse o yazar:
 *    HIZLI YOL      cüzdanın senkron cevabı
 *    OLAY YOLU      cüzdanın "wallet.events" olayı (sohbet dışından ödeme, sonradan iade de buradan)
 *    GÜVENLİK AĞI   reconciler: PROCESSING'de takılanları aynı anahtarla cüzdana tekrar sorar
 * </pre>
 *
 * Her başarılı geçiş aynı transaction'da "payment.events" olayı yazar (outbox). Sohbet servisi bu olaylardan kartı
 * kurar ve günceller; para mantığından haberi yoktur.
 */
@Service
public class PaymentRequestService {

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestService.class);

    public record CreateCommand(long requesterId, long payerId, long amountKurus, String description,
                                String originKind, String originRef, String idempotencyKey) {
    }

    private final PaymentRequestRepository repo;
    private final WalletClient wallet;
    private final Outbox outbox;
    private final TransactionTemplate tx;
    private final PayReqProperties props;
    private final MeterRegistry meters;

    public PaymentRequestService(PaymentRequestRepository repo, WalletClient wallet, Outbox outbox,
                                 TransactionTemplate tx, PayReqProperties props, MeterRegistry meters) {
        this.repo = repo;
        this.wallet = wallet;
        this.outbox = outbox;
        this.tx = tx;
        this.props = props;
        this.meters = meters;
    }

    public static String walletKey(long id) {
        return "payreq-" + id;
    }

    // ------------------------------------------------------------------ oluştur

    public PaymentRequest create(CreateCommand c) {
        if (c.amountKurus() <= 0 || c.amountKurus() > 5_000_000) {
            throw new PaymentException("BAD_REQUEST", "tutar 0,01 TL ile 50.000 TL arasında olmalı");
        }
        if (c.payerId() == c.requesterId()) {
            throw new PaymentException("BAD_REQUEST", "kendinden para isteyemezsin");
        }
        if (c.idempotencyKey() == null || c.idempotencyKey().isBlank()) {
            throw new PaymentException("BAD_REQUEST", "Idempotency-Key zorunlu");
        }
        if (!java.util.Set.of("CHAT", "LINK", "INVOICE", "APP").contains(c.originKind())) {
            throw new PaymentException("BAD_REQUEST", "bilinmeyen kaynak: " + c.originKind());
        }
        PaymentRequest created = tx.execute(s -> repo.create(c.requesterId(), c.payerId(), c.amountKurus(),
                        c.description() == null ? "" : c.description(), c.originKind(), c.originRef(), c.idempotencyKey(),
                        Instant.now().plus(props.requestTtl()))
                .map(pr -> {
                    outbox.publish(PaymentRequestEvent.of("CREATED", pr));
                    return pr;
                }).orElse(null));
        if (created != null) {
            meters.counter("payreq.created", "origin", c.originKind()).increment();
            return created;
        }
        return repo.byIdempotencyKey(c.requesterId(), c.idempotencyKey()).orElseThrow();   // tekrar: ilk sonucu dön
    }

    // ------------------------------------------------------------------ öde / reddet

    /** Dönüş: PAID, PROCESSING (sonuç bekleniyor ya da başka cihaz ödüyor), ALREADY_PAID, EXPIRED, DECLINED, REVERSED, FAILED:&lt;sebep&gt; */
    public String pay(long userId, long id) {
        PaymentRequest pr = repo.find(id).orElseThrow(() -> new PaymentException("NOT_FOUND", "para isteği yok"));
        if (pr.payerId() != userId) {
            throw new PaymentException("FORBIDDEN", "bu isteği sadece istenen kişi ödeyebilir");
        }
        return repo.claim(id, userId).map(this::complete).orElseGet(() -> {
            String now = repo.find(id).orElseThrow().status();
            return switch (now) {
                case "PAID" -> "ALREADY_PAID";
                case "PENDING" -> "EXPIRED";             // kapma başarısız ama PENDING: süresi dolmuş, job henüz kapatmamış
                default -> now;
            };
        });
    }

    /** ② ve ③. İlk ödemede ve reconciler'da aynı yol: cüzdan idempotent olduğu için tekrar güvenli. */
    public String complete(PaymentRequest pr) {
        TransferResult r;
        try {
            r = wallet.transfer(walletKey(pr.id()), pr.payerId(), pr.requesterId(), pr.amountKurus());
        } catch (WalletUnavailableException e) {
            log.warn("Para isteği {} için cüzdan cevabı belirsiz; olay ya da reconciler kapatacak: {}", pr.id(), e.getMessage());
            meters.counter("payreq.uncertain").increment();
            return "PROCESSING";
        }
        if (!r.ok()) {
            repo.release(pr.id());
            return "FAILED:" + r.rejectReason();
        }
        tx.executeWithoutResult(s -> repo.markPaid(pr.id(), r.transferId())
                .ifPresent(paid -> outbox.publish(PaymentRequestEvent.of("PAID", paid))));
        return "PAID";                                  // markPaid boş döndüyse cüzdan olayı daha önce PAID yapmıştı
    }

    public boolean decline(long userId, long id) {
        PaymentRequest pr = repo.find(id).orElseThrow(() -> new PaymentException("NOT_FOUND", "para isteği yok"));
        if (pr.payerId() != userId) {
            throw new PaymentException("FORBIDDEN", "bu isteği sadece istenen kişi reddedebilir");
        }
        return Boolean.TRUE.equals(tx.execute(s -> publishIfPresent("DECLINED", repo.decline(id, userId))));
    }

    public PaymentRequest get(long userId, long id) {
        PaymentRequest pr = repo.find(id).orElseThrow(() -> new PaymentException("NOT_FOUND", "para isteği yok"));
        if (pr.payerId() != userId && pr.requesterId() != userId) {
            throw new PaymentException("NOT_FOUND", "para isteği yok");    // başkasının isteğinin varlığını bile söyleme
        }
        return pr;
    }

    // ------------------------------------------------------------------ cüzdan olayları ve işler

    boolean onTransferCompleted(long id, String transferId, long amountKurus) {
        PaymentRequest pr = repo.find(id).orElse(null);
        if (pr == null) {
            return false;
        }
        if (pr.amountKurus() != amountKurus) {
            log.error("Para isteği {} için tutar uyuşmazlığı: istek {} kuruş, transfer {} kuruş ({}). Durum değiştirilmedi.",
                    id, pr.amountKurus(), amountKurus, transferId);
            meters.counter("payreq.amount_mismatch").increment();
            return false;
        }
        return Boolean.TRUE.equals(tx.execute(s -> publishIfPresent("PAID", repo.markPaidByWallet(id, transferId, amountKurus))));
    }

    boolean onTransferReversed(String transferId, String reason) {
        return Boolean.TRUE.equals(tx.execute(s -> publishIfPresent("REVERSED", repo.markReversed(transferId, reason))));
    }

    public void expire(long id) {
        tx.executeWithoutResult(s -> publishIfPresent("EXPIRED", repo.expire(id)));
    }

    private boolean publishIfPresent(String type, Optional<PaymentRequest> changed) {
        changed.ifPresent(pr -> outbox.publish(PaymentRequestEvent.of(type, pr)));
        return changed.isPresent();
    }
}
