package io.paramix.notify.api;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

import io.paramix.notify.domain.NotificationEvent;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.domain.Topics;
import io.paramix.notify.store.CampaignStore;
import io.paramix.notify.store.NotificationRepository;
import io.paramix.notify.store.OutboxRepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * ① API katmanı: idempotency + transactional outbox.
 * Bildirim satırı ile outbox satırı aynı transaction'da yazılır: ya ikisi ya hiçbiri.
 * Kafka'ya doğrudan yazmıyoruz; DB commit olup Kafka'ya yazmadan çökersek bildirim kaybolurdu.
 */
@Service
public class NotificationService {

    public record Accepted(String notificationId, boolean duplicate) {
    }

    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final CampaignStore campaigns;
    private final ObjectMapper json;
    private final Clock clock;

    public NotificationService(NotificationRepository notifications, OutboxRepository outbox, CampaignStore campaigns,
                               ObjectMapper json, Clock clock) {
        this.notifications = notifications;
        this.outbox = outbox;
        this.campaigns = campaigns;
        this.json = json;
        this.clock = clock;
    }

    @Transactional
    public Accepted accept(NotificationRequest r) {
        if (r.campaignId() != null && !campaigns.exists(r.campaignId())) {
            throw new IllegalArgumentException("Bilinmeyen kampanya: " + r.campaignId());
        }
        if (r.priority() == Priority.MARKETING && r.fallback()) {
            throw new IllegalArgumentException("Pazarlama bildiriminde fallback kullanılamaz");
        }
        String id = "ntf_" + UUID.randomUUID().toString().replace("-", "");
        NotificationEvent event = new NotificationEvent(id, r.idempotencyKey(), r.type(), r.priority(), r.userId(),
                r.templateId(), r.params() == null ? Map.of() : r.params(), r.channels(), r.fallback(),
                r.campaignId(), clock.instant());
        String payload = write(event);

        boolean inserted = notifications.insertIfAbsent(id, r.idempotencyKey(), r.type(), r.priority().name(),
                r.userId(), r.campaignId(), payload);
        if (!inserted) {
            String existing = notifications.findIdByKey(r.idempotencyKey()).orElseThrow();
            return new Accepted(existing, true);
        }
        outbox.insert(Topics.ingress(r.priority()), r.userId(), payload);
        return new Accepted(id, false);
    }

    private String write(Object o) {
        try {
            return json.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
