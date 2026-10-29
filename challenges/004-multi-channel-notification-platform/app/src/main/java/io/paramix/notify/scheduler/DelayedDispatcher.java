package io.paramix.notify.scheduler;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.messaging.DeliveryRequeue;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Zamanı gelen ertelenmiş teslimatları yayınlar.
 * <ul>
 *   <li>SCHEDULED (sessiz saat bitti) → PENDING → teslimat topic'i.</li>
 *   <li>FALLBACK_WAIT (ör. OTP SMS yedeği): aynı bildirimin push'u ack aldıysa SKIPPED, almadıysa gönder.
 *       İçerik aynı (OTP kodu Auth servisinde bir kez üretildi): kullanıcı iki kanaldan da aynı kodu görür.</li>
 * </ul>
 * Geçişler koşullu UPDATE + outbox (aynı transaction): birden fazla scheduler pod'u aynı satırı iki kez
 * yayınlamaz, arada çökme satırı PENDING'de yetim bırakmaz.
 */
@Component
@ConditionalOnProperty(name = "notify.roles.scheduler", havingValue = "true", matchIfMissing = true)
public class DelayedDispatcher {

    private final DeliveryRepository deliveries;
    private final DeliveryRequeue requeue;

    public DelayedDispatcher(DeliveryRepository deliveries, DeliveryRequeue requeue) {
        this.deliveries = deliveries;
        this.requeue = requeue;
    }

    @Scheduled(fixedDelayString = "${notify.scheduler.delayed-ms:1000}")
    public void dispatch() {
        for (Delivery d : deliveries.findDue(DeliveryStatus.SCHEDULED, 500)) {
            requeue.promote(d, DeliveryStatus.SCHEDULED);
        }
        for (Delivery d : deliveries.findDue(DeliveryStatus.FALLBACK_WAIT, 500)) {
            boolean primaryAcked = deliveries.findByNotification(d.notificationId()).stream()
                    .anyMatch(o -> o.channel() == Channel.PUSH && o.status() == DeliveryStatus.DELIVERED);
            if (primaryAcked) {
                deliveries.markSkipped(d.deliveryId(), "primary_acknowledged", DeliveryStatus.FALLBACK_WAIT);
            } else {
                requeue.promote(d, DeliveryStatus.FALLBACK_WAIT);
            }
        }
    }
}
