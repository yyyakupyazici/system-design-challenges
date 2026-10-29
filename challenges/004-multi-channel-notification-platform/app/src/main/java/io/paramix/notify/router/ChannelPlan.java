package io.paramix.notify.router;

import java.time.Instant;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryStatus;

/** Router'ın bir kanal için verdiği karar: hemen gönder, ertele, yedekte beklet ya da atla. */
public record ChannelPlan(Channel channel, String recipient, DeliveryStatus status, Instant notBefore, String reason) {

    static ChannelPlan pending(Channel c, String recipient) {
        return new ChannelPlan(c, recipient, DeliveryStatus.PENDING, null, null);
    }

    static ChannelPlan skipped(Channel c, String reason) {
        return new ChannelPlan(c, null, DeliveryStatus.SKIPPED, null, reason);
    }
}
