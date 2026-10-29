package io.paramix.notify.scheduler;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.messaging.DeliveryRequeue;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.worker.Reconciler;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Pod gönderirken öldüyse satır SENDING'de kalır ve kilidi dolar. Reaper:
 * <ul>
 *   <li>Sağlayıcı seçilmeden öldüyse (provider boş) → kesin gönderilmedi → PENDING, tekrar yayınla.</li>
 *   <li>Sağlayıcı seçildiyse → UNKNOWN yap, sağlayıcıya sor (reconcile).</li>
 *   <li>Inline reconcile'ı başarısız olmuş eski UNKNOWN'lar → tekrar reconcile.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "notify.roles.scheduler", havingValue = "true", matchIfMissing = true)
public class StuckDeliveryReaper {

    private final DeliveryRepository deliveries;
    private final DeliveryRequeue requeue;
    private final Reconciler reconciler;
    private final NotifyProperties props;

    public StuckDeliveryReaper(DeliveryRepository deliveries, DeliveryRequeue requeue, Reconciler reconciler,
                               NotifyProperties props) {
        this.deliveries = deliveries;
        this.requeue = requeue;
        this.reconciler = reconciler;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${notify.scheduler.reaper-ms:5000}")
    public void reap() {
        for (Delivery d : deliveries.findStuck(props.delivery().unknownGrace(), 100)) {
            if (d.status() == DeliveryStatus.SENDING && d.provider() == null) {
                requeue.releaseExpired(d, "lease_expired_before_send");
                continue;
            }
            if (d.status() == DeliveryStatus.SENDING) {
                if (!deliveries.markUnknownIfLeaseExpired(d.deliveryId(), "lease_expired_during_send")) {
                    continue;
                }
            }
            deliveries.find(d.deliveryId())
                    .filter(x -> x.status() == DeliveryStatus.UNKNOWN)
                    .ifPresent(reconciler::reconcile);
        }
    }
}
