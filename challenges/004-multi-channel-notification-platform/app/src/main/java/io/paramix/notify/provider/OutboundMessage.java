package io.paramix.notify.provider;

import io.paramix.notify.domain.Channel;

/** Sağlayıcıya giden mesaj. clientRef = deliveryId: sağlayıcı tarafında mükerrer kontrolü ve durum sorgusu için. */
public record OutboundMessage(String clientRef, Channel channel, String recipient, String title, String body) {
}
