package io.paramix.notify.messaging;

import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.Topics;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.store.OutboxRepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Bir teslimatı tekrar kuyruğa almak = durumu PENDING yapmak + teslimat topic'ine yazmak.
 * İkisi ayrı adımlarsa arada çökme satırı sonsuza kadar PENDING bırakır (kuyrukta mesajı yok).
 * Bu yüzden aynı DB transaction'ında outbox'a yazılır; relay Kafka'ya taşır.
 * Kullananlar düşük hacimli: sessiz saat / yedek kanal zamanlayıcısı, reconcile, reaper.
 */
@Service
public class DeliveryRequeue {

    private final DeliveryRepository deliveries;
    private final OutboxRepository outbox;
    private final ObjectMapper json;

    public DeliveryRequeue(DeliveryRepository deliveries, OutboxRepository outbox, ObjectMapper json) {
        this.deliveries = deliveries;
        this.outbox = outbox;
        this.json = json;
    }

    /** from → PENDING (koşullu) ve outbox; geçiş başka pod tarafından yapılmışsa hiçbir şey yazılmaz. */
    @Transactional
    public boolean promote(Delivery d, DeliveryStatus from) {
        if (!deliveries.promoteToPending(d.deliveryId(), from)) {
            return false;
        }
        enqueue(d);
        return true;
    }

    /** Reaper: kilit gerçekten dolmuş SENDING satırı (sağlayıcı seçilmeden ölmüş pod) → PENDING + outbox. */
    @Transactional
    public boolean releaseExpired(Delivery d, String reason) {
        if (!deliveries.releaseExpiredLease(d.deliveryId(), reason)) {
            return false;
        }
        enqueue(d);
        return true;
    }

    private void enqueue(Delivery d) {
        try {
            outbox.insert(Topics.delivery(d.channel(), d.priority()), d.userId(), json.writeValueAsString(d.toMessage()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
