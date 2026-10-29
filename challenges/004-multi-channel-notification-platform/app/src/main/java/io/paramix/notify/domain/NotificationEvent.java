package io.paramix.notify.domain;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** API'nin outbox üzerinden {@code notif.<öncelik>} topic'ine yazdığı mesaj; router'ın girdisi. */
public record NotificationEvent(
        String notificationId,
        String idempotencyKey,
        String type,
        Priority priority,
        String userId,
        String templateId,
        Map<String, String> params,
        List<Channel> channels,
        boolean fallback,
        String campaignId,
        Instant createdAt) {
}
