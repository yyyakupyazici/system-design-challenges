package io.paramix.provision.trade;

import java.util.Map;
import java.util.UUID;

import io.paramix.provision.domain.DomainException;
import io.paramix.provision.domain.ErrorCode;
import io.paramix.provision.domain.Provision;
import io.paramix.provision.domain.ProvisionService;
import io.paramix.provision.inbox.Inbox;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Emir olaylarını provizyona uygular. Kafka'dan bağımsız: dinleyici sadece mesajı çözüp buraya verir, böylece
 * kural Kafka olmadan da test edilebilir.
 *
 * <pre>
 *  BEGIN
 *    ① INBOX     INSERT inbox (consumer, event_id) ON CONFLICT DO NOTHING  → çakışma: DUPLICATE, hiçbir şey yapma
 *    ② ETKİ      SAVEPOINT · capture / release (provizyon + bakiye + ledger + outbox)
 *                └─ durumla çelişiyor (iptal edilmiş emre dolum) → ROLLBACK TO SAVEPOINT, inbox = CONFLICT, alarm
 *  COMMIT        → sonra Kafka offset'i commit edilir
 * </pre>
 *
 * Offset commit'ten önce ölürsek mesaj tekrar gelir ve ① onu DUPLICATE olarak yakalar.
 *
 * <p><b>Inbox neden gerekli, durum makinesi zaten "HELD değilse dokunma" demiyor mu?</b> Diyor, para iki kez
 * hareket etmez. Ama inbox olmadan "aynı iptal olayının ikinci teslimi" ile "iptal edilmiş emre gelen YENİ bir
 * dolum olayı" aynı görünür: ikisi de "provizyon HELD değil". İlki zararsız tekrar, ikincisi ise emir motorunun
 * serbest bırakılmış parayla işlem yaptığı anlamına gelen gerçek bir para hatası. Inbox ilkini sessizce atlar,
 * ikincisini CONFLICT olarak kaydedip alarm üretir. Inbox olmasaydı ya her tekrar yanlış alarm üretirdi ya da
 * gerçek çelişkiler gürültüde kaybolurdu.
 */
@Component
public class TradeEventHandler {

    public static final String CONSUMER = "provision.trade-events";

    private static final Logger log = LoggerFactory.getLogger(TradeEventHandler.class);

    public enum Outcome { APPLIED, DUPLICATE, CONFLICT }

    /** Provizyon henüz görünmüyor (olay, provizyonun commit'inden önce geldi). Tekrar denemek düzeltebilir. */
    public static class ProvisionNotYetVisibleException extends RuntimeException {
        public ProvisionNotYetVisibleException(String orderId) {
            super("Emir " + orderId + " için provizyon henüz yok");
        }
    }

    private final Inbox inbox;
    private final ProvisionService provisions;
    private final TransactionTemplate tx;
    private final TransactionTemplate savepoint;
    private final MeterRegistry meters;

    public TradeEventHandler(Inbox inbox, ProvisionService provisions, PlatformTransactionManager txManager,
                             MeterRegistry meters) {
        this.inbox = inbox;
        this.provisions = provisions;
        this.tx = new TransactionTemplate(txManager);
        this.savepoint = new TransactionTemplate(txManager);
        this.savepoint.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        this.meters = meters;
    }

    public Outcome handle(UUID eventId, TradeEvent event) {
        Outcome outcome = tx.execute(status -> {
            if (!inbox.tryRecord(CONSUMER, eventId, event.type().name())) {
                return Outcome.DUPLICATE;
            }
            try {
                savepoint.executeWithoutResult(s -> apply(event));
                return Outcome.APPLIED;
            } catch (DomainException e) {
                // Tekrar denemek bunu düzeltmez: kaydet, alarm üret, mesajı tüket (partition'ı tıkama).
                inbox.markConflict(CONSUMER, eventId, e.code() + ": " + e.getMessage());
                log.error("EMİR OLAYI ÇELİŞKİSİ event={} {} order={} müşteri={}: {} {}", eventId, event.type(),
                        event.orderId(), event.customerId(), e.code(), e.getMessage());
                return Outcome.CONFLICT;
            }
        });
        meters.counter("inbox.messages", "consumer", CONSUMER, "outcome", outcome.name()).increment();
        return outcome;
    }

    private void apply(TradeEvent event) {
        Provision p = provisions.findByOperation(event.customerId(), Provision.OperationType.TRADE, event.orderId())
                .orElseThrow(() -> new ProvisionNotYetVisibleException(event.orderId()));
        switch (event.type()) {
            case ORDER_FILLED -> {
                if (event.filledAmount() == null) {
                    throw new DomainException(ErrorCode.INVALID_AMOUNT,
                            "ORDER_FILLED olayında filledAmount yok", Map.of("orderId", event.orderId()));
                }
                provisions.capture(event.customerId(), p.id(), event.filledAmount());
            }
            case ORDER_CANCELLED -> provisions.release(event.customerId(), p.id());
        }
    }
}
