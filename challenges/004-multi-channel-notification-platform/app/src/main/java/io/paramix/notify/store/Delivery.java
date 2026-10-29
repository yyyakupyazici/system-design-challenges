package io.paramix.notify.store;

import java.time.Instant;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.Priority;

public record Delivery(
        String deliveryId,
        String notificationId,
        String userId,
        Channel channel,
        Priority priority,
        String campaignId,
        String recipient,
        String title,
        String body,
        DeliveryStatus status,
        String provider,
        String providerMsgId,
        int attempt,
        Instant notBefore,
        Instant lockedUntil,
        String lastError,
        Instant updatedAt) {

    public DeliveryMessage toMessage() {
        return new DeliveryMessage(deliveryId, notificationId, userId, channel, priority, campaignId);
    }
}
