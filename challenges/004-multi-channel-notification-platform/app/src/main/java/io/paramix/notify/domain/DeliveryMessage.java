package io.paramix.notify.domain;

/**
 * Teslimat topic'lerindeki mesaj. Bilerek ince: içerik DB'deki delivery satırında,
 * worker satırı sahiplenirken (UPDATE ... RETURNING) zaten okuyor.
 */
public record DeliveryMessage(String deliveryId, String notificationId, String userId,
                              Channel channel, Priority priority, String campaignId) {

    /** ② Router katmanı idempotency: aynı bildirim + kanal her zaman aynı teslimat ID'sini üretir. */
    public static String deliveryId(String notificationId, Channel channel) {
        return notificationId + ":" + channel.name();
    }
}
