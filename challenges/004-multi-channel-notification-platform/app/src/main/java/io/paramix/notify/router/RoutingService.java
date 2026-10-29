package io.paramix.notify.router;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.NotificationEvent;
import io.paramix.notify.messaging.DeliveryPublisher;
import io.paramix.notify.store.CampaignStore;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.store.TemplateStore;
import io.paramix.notify.store.UserProfile;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Router: kime, hangi kanaldan, hangi içerikle, ne zaman? Sağlayıcıyı çağırmaz, kota almaz.
 *
 * <p>Aynı mesaj iki kez işlenirse (offset commit edilmeden çöktü): delivery_id deterministik olduğu için
 * satırlar bir kez oluşur; Kafka'ya tekrar yazılabilir ama worker sahiplenme adımında eler.
 */
@Service
public class RoutingService {

    private static final Logger log = LoggerFactory.getLogger(RoutingService.class);

    private final UserDirectory users;
    private final TemplateStore templates;
    private final CampaignStore campaigns;
    private final DeliveryRepository deliveries;
    private final DeliveryPublisher publisher;
    private final NotifyProperties props;
    private final Clock clock;
    private final MeterRegistry meters;

    public RoutingService(UserDirectory users, TemplateStore templates, CampaignStore campaigns,
                          DeliveryRepository deliveries, DeliveryPublisher publisher, NotifyProperties props,
                          Clock clock, MeterRegistry meters) {
        this.users = users;
        this.templates = templates;
        this.campaigns = campaigns;
        this.deliveries = deliveries;
        this.publisher = publisher;
        this.props = props;
        this.clock = clock;
        this.meters = meters;
    }

    public void route(NotificationEvent e) {
        if (e.campaignId() != null && campaigns.isStopped(e.campaignId())) {
            meters.counter("notify.router.dropped", "reason", "campaign_stopped").increment();
            return;
        }
        Optional<UserProfile> user = users.get(e.userId());
        if (user.isEmpty()) {
            log.warn("Bilinmeyen kullanıcı, bildirim atlandı: {} {}", e.notificationId(), e.userId());
            meters.counter("notify.router.dropped", "reason", "unknown_user").increment();
            return;
        }
        List<ChannelPlan> plans = RoutingPolicy.decide(e, user.get(), clock.instant(), props.delivery().fallbackWait());
        for (ChannelPlan plan : plans) {
            Delivery d = toDelivery(e, user.get(), plan);
            deliveries.insertIfAbsent(d);
            meters.counter("notify.router.planned", "priority", e.priority().name(),
                    "channel", plan.channel().name(), "status", d.status().name()).increment();
            if (d.status() == DeliveryStatus.PENDING) {
                // Tekrar işlemede satır zaten ilerlemiş olabilir (SENT vb.): sadece hâlâ PENDING ise yayınla.
                deliveries.find(d.deliveryId())
                        .filter(current -> current.status() == DeliveryStatus.PENDING)
                        .ifPresent(current -> publisher.publish(current.toMessage()));
            }
        }
    }

    private Delivery toDelivery(NotificationEvent e, UserProfile user, ChannelPlan plan) {
        String id = DeliveryMessage.deliveryId(e.notificationId(), plan.channel());
        DeliveryStatus status = plan.status();
        String reason = plan.reason();
        String title = null;
        String body = null;
        if (status != DeliveryStatus.SKIPPED) {
            Optional<TemplateStore.Template> t = templates.find(e.templateId(), user.locale(), plan.channel());
            if (t.isEmpty()) {
                status = DeliveryStatus.SKIPPED;
                reason = "no_template";
            } else {
                try {
                    title = TemplateRenderer.render(t.get().title(), e.params());
                    body = TemplateRenderer.render(t.get().body(), e.params());
                } catch (TemplateRenderer.MissingParameterException ex) {
                    status = DeliveryStatus.FAILED;
                    reason = ex.getMessage();
                }
            }
        }
        return new Delivery(id, e.notificationId(), e.userId(), plan.channel(), e.priority(), e.campaignId(),
                plan.recipient(), title, body, status, null, null, 0, plan.notBefore(), null, reason, null);
    }
}
