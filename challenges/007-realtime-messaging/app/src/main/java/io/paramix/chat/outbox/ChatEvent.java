package io.paramix.chat.outbox;

import java.util.Map;

/**
 * Outbox → Kafka → fan-out arasında taşınan olay. Fan-out worker'ın bilmesi gereken her şey burada:
 * kime (sohbetin üyeleri), hangi çerçeve, gönderen cihaz hariç mi, çevrimdışılara push gidecek mi.
 *
 * @param excludeDevice "userId:deviceId": mesajı gönderen cihaz (zaten ✓ ile biliyor, ona tekrar gitmesin)
 * @param pushPreview   çevrimdışı üyelere gidecek bildirim metni; null ise push yok (okundu bilgisi gibi)
 */
public record ChatEvent(long conversationId, String conversationKind, long actorUserId, String excludeDevice,
                        String pushPreview, Map<String, Object> frame) {
}
