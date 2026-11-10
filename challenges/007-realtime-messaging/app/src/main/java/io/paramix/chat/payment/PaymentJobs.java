package io.paramix.chat.payment;

import io.paramix.chat.config.ChatProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Takılı ödemeleri tamamlar ve süresi dolan istekleri kapatır. Birden çok pod'da çalışsa da güvenli:
 * cüzdan idempotent, durum geçişleri koşullu UPDATE, sistem mesajları deterministik id'li.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.payment", havingValue = "true", matchIfMissing = true)
public class PaymentJobs {

    private static final Logger log = LoggerFactory.getLogger(PaymentJobs.class);

    private final PaymentRepository repo;
    private final PaymentRequestService service;
    private final ChatProperties props;

    public PaymentJobs(PaymentRepository repo, PaymentRequestService service, ChatProperties props) {
        this.repo = repo;
        this.service = service;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${chat.payment.reconcile-every:2s}")
    public void reconcile() {
        for (PaymentRepository.PaymentRequest pr : repo.stuckProcessing(props.payment().reconcileAfter().toMillis())) {
            String r = service.complete(pr);
            log.info("Para isteği {} reconcile edildi: {}", pr.id(), r);
        }
        for (Long id : repo.expiredPending()) {
            service.expire(id);
        }
    }
}
