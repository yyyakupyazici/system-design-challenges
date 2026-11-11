package io.paramix.payreq.domain;

import io.paramix.payreq.config.PayReqProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Güvenlik ağı: takılan ödemeleri tamamlar, süresi dolanları kapatır. Birden çok pod'da çalışsa da güvenli. */
@Component
public class PaymentJobs {

    private static final Logger log = LoggerFactory.getLogger(PaymentJobs.class);

    private final PaymentRequestRepository repo;
    private final PaymentRequestService service;
    private final PayReqProperties props;

    public PaymentJobs(PaymentRequestRepository repo, PaymentRequestService service, PayReqProperties props) {
        this.repo = repo;
        this.service = service;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${payreq.reconcile-every:2s}")
    public void run() {
        for (PaymentRequest pr : repo.stuckProcessing(props.reconcileAfter().toMillis())) {
            log.info("Para isteği {} reconcile edildi: {}", pr.id(), service.complete(pr));
        }
        repo.expiredPending().forEach(service::expire);
    }
}
