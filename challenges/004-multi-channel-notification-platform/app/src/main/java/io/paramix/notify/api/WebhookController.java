package io.paramix.notify.api;

import java.util.Map;

import io.paramix.notify.store.DeliveryRepository;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * ④ Sağlayıcı teslim raporları ve uygulama ack'leri. Aynı rapor defalarca gelebilir:
 * durum sadece ileri gider, tekrarlar 200 döner ve hiçbir şey değiştirmez.
 * (Gerçek sistemde webhook imzası doğrulanır.)
 */
@RestController
@ConditionalOnProperty(name = "notify.roles.api", havingValue = "true", matchIfMissing = true)
public class WebhookController {

    public record DeliveryReport(@NotBlank String providerMsgId,
                                 @Pattern(regexp = "DELIVERED|FAILED") String status,
                                 String error) {
    }

    private final DeliveryRepository deliveries;

    public WebhookController(DeliveryRepository deliveries) {
        this.deliveries = deliveries;
    }

    @PostMapping("/webhooks/{provider}/dlr")
    public ResponseEntity<Map<String, Object>> report(@PathVariable String provider, @Valid @RequestBody DeliveryReport r) {
        boolean changed = "DELIVERED".equals(r.status())
                ? deliveries.markDeliveredByProvider(provider, r.providerMsgId())
                : deliveries.markFailedByProvider(provider, r.providerMsgId(), r.error());
        return ResponseEntity.ok(Map.of("changed", changed));
    }

    /** Uygulama push'u gösterdiğinde çağırır; bekleyen SMS yedeğini gereksiz kılar. */
    @PostMapping("/deliveries/{deliveryId}/ack")
    public ResponseEntity<Map<String, Object>> ack(@PathVariable String deliveryId) {
        return ResponseEntity.ok(Map.of("changed", deliveries.acknowledge(deliveryId)));
    }
}
