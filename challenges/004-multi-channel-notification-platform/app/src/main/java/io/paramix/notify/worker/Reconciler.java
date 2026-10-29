package io.paramix.notify.worker;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.messaging.DeliveryRequeue;
import io.paramix.notify.provider.LookupResult;
import io.paramix.notify.provider.ProviderRegistry;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * "Gitti mi bilmiyorum" (UNKNOWN) durumunu çözer:
 * <ol>
 *   <li>Sağlayıcıya sor: bu clientRef ile gönderdin mi?</li>
 *   <li>Evet → SENT (tekrar gönderme). Hayır → güvenle tekrar gönder.</li>
 *   <li>Sorgulanamıyor → öncelik politikası: OTP tekrar gönderilir, kampanya bırakılır.</li>
 * </ol>
 */
@Component
public class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    private final ProviderRegistry providers;
    private final DeliveryRepository deliveries;
    private final DeliveryRequeue requeue;
    private final NotifyProperties props;
    private final MeterRegistry meters;

    public Reconciler(ProviderRegistry providers, DeliveryRepository deliveries, DeliveryRequeue requeue,
                      NotifyProperties props, MeterRegistry meters) {
        this.providers = providers;
        this.deliveries = deliveries;
        this.requeue = requeue;
        this.props = props;
        this.meters = meters;
    }

    /** Ön koşul: satır UNKNOWN ve provider dolu. */
    public void reconcile(Delivery d) {
        LookupResult result = providers.get(d.provider()).lookup(d.deliveryId());
        String outcome;
        switch (result) {
            case LookupResult.Found f -> {
                deliveries.markSent(d.deliveryId(), d.provider(), f.providerMsgId(), DeliveryStatus.UNKNOWN);
                outcome = "found_sent";
            }
            case LookupResult.NotFound n -> {
                resend(d);
                outcome = "not_found_resent";
            }
            case LookupResult.Unavailable u -> {
                if (DeliveryPolicy.onUnknown(d.priority()) == DeliveryPolicy.UnknownAction.RESEND) {
                    resend(d);
                    outcome = "unknown_resent";
                } else {
                    deliveries.markFailed(d.deliveryId(), "unknown_abandoned", DeliveryStatus.UNKNOWN);
                    outcome = "unknown_abandoned";
                }
            }
        }
        meters.counter("notify.reconcile", "priority", d.priority().name(), "outcome", outcome).increment();
        log.info("Reconcile {} ({}) → {}", d.deliveryId(), d.priority(), outcome);
    }

    private void resend(Delivery d) {
        if (d.attempt() >= props.delivery().maxAttempts()) {
            deliveries.markFailed(d.deliveryId(), "max_attempts", DeliveryStatus.UNKNOWN);
            return;
        }
        requeue.promote(d, DeliveryStatus.UNKNOWN);
    }
}
