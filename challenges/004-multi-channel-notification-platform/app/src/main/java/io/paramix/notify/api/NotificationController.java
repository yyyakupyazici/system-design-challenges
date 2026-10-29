package io.paramix.notify.api;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.store.NotificationRepository;

import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = "notify.roles.api", havingValue = "true", matchIfMissing = true)
public class NotificationController {

    private final NotificationService service;
    private final NotificationRepository notifications;
    private final DeliveryRepository deliveries;

    public NotificationController(NotificationService service, NotificationRepository notifications,
                                  DeliveryRepository deliveries) {
        this.service = service;
        this.notifications = notifications;
        this.deliveries = deliveries;
    }

    /** Hızlıca kuyruğa alır ve 202 döner; gönderim asenkron. Aynı idempotencyKey → aynı notificationId. */
    @PostMapping("/notifications")
    public ResponseEntity<NotificationService.Accepted> create(@Valid @RequestBody NotificationRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.accept(request));
    }

    /** Müşteri hizmetleri: "bu kullanıcıya OTP gitti mi?" */
    @GetMapping("/notifications/{id}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String id) {
        return notifications.find(id).map(n -> {
            Map<String, Object> body = new LinkedHashMap<>(n);
            List<Map<String, Object>> ds = deliveries.findByNotification(id).stream().map(NotificationController::view).toList();
            body.put("deliveries", ds);
            return ResponseEntity.ok(body);
        }).orElse(ResponseEntity.notFound().build());
    }

    private static Map<String, Object> view(Delivery d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("deliveryId", d.deliveryId());
        m.put("channel", d.channel());
        m.put("status", d.status());
        m.put("provider", d.provider());
        m.put("providerMsgId", d.providerMsgId());
        m.put("attempt", d.attempt());
        m.put("notBefore", d.notBefore());
        m.put("lastError", d.lastError());
        m.put("updatedAt", d.updatedAt());
        return m;
    }
}
