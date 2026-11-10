package io.paramix.chat.message;

import java.util.Map;

/**
 * İstemcinin gördüğü mesaj. Kimliği (conversationId, seq); istemci ekranda bu ikiliyle tekilleştirir.
 *
 * @param serverTime ekranda gösterilen saat SUNUCUNUN saatidir; telefon saatine güvenilmez
 * @param paymentRequest para isteği kartının güncel durumu (id, status, amountKurus, payerId), yoksa null
 */
public record MessageView(long conversationId, long seq, long senderId, String clientMsgId, String kind, String body,
                          String serverTime, Map<String, Object> paymentRequest) {
}
