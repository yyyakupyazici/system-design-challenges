package io.paramix.chat.protocol;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.paramix.chat.message.MessageView;

/**
 * Sunucudan istemciye giden çerçeveler (WebSocket üzerinden JSON). Her çerçevenin bir "type" alanı var.
 *
 * <pre>
 *  İstemci → sunucu:  SEND, ACK, READ, SYNC, HEADS, TYPING, PRESENCE_QUERY, PING
 *  Sunucu → istemci:  SENT (✓), MSG, RECEIPT (✓✓ / okundu), SYNC_RESULT, TYPING, PRESENCE,
 *                     RECONNECT (gateway kapanıyor: şu kadar bekleyip yeniden bağlan), ERROR, PONG
 * </pre>
 */
public final class Frames {

    private Frames() {
    }

    public static Map<String, Object> of(String type, Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                m.put((String) kv[i], kv[i + 1]);
            }
        }
        return m;
    }

    /** ✓: mesaj kalıcı olarak kaydedildi. Tekrar gönderilen mesajda da aynı seq döner (duplicate=true). */
    public static Map<String, Object> sent(String clientMsgId, long conversationId, long seq, String serverTime, boolean duplicate) {
        return of("SENT", "clientMsgId", clientMsgId, "conversationId", conversationId, "seq", seq,
                "serverTime", serverTime, "duplicate", duplicate);
    }

    public static Map<String, Object> msg(MessageView m) {
        return of("MSG", "message", m);
    }

    /** receipt: DELIVERED (✓✓) ya da READ (mavi ✓✓). userId = alan/okuyan kişi. */
    public static Map<String, Object> receipt(long conversationId, long userId, long seq, String receipt) {
        return of("RECEIPT", "conversationId", conversationId, "userId", userId, "seq", seq, "receipt", receipt);
    }

    public static Map<String, Object> syncResult(List<Map<String, Object>> conversations) {
        return of("SYNC_RESULT", "conversations", conversations);
    }

    public static Map<String, Object> error(String code, String message, String ref) {
        return of("ERROR", "code", code, "message", message, "ref", ref);
    }
}
