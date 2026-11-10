package io.paramix.chat.fanout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.paramix.chat.message.ChatException;
import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.protocol.Frames;

import org.springframework.stereotype.Component;

/**
 * "Yazıyor..." gibi GEÇİCİ sinyaller: veritabanına yazılmaz, Kafka'dan geçmez, kaybolursa kimse fark etmez.
 * Doğrudan yönlendirme tablosu + Pub/Sub. Kişi başı 2 saniyede bir ile sınırlı (her tuş vuruşunda değil).
 */
@Component
public class EphemeralRelay {

    private static final long TYPING_INTERVAL_MS = 2000;

    private final ConversationRepository conversations;
    private final DeliveryRouter router;
    private final DeliveryPublisher publisher;
    private final Map<String, Long> lastTyping = new ConcurrentHashMap<>();

    public EphemeralRelay(ConversationRepository conversations, DeliveryRouter router, DeliveryPublisher publisher) {
        this.conversations = conversations;
        this.router = router;
        this.publisher = publisher;
    }

    public void typing(long userId, String deviceId, long conversationId) {
        long now = System.currentTimeMillis();
        String k = userId + ":" + conversationId;
        Long prev = lastTyping.get(k);
        if (prev != null && now - prev < TYPING_INTERVAL_MS) {
            return;
        }
        lastTyping.put(k, now);
        List<Long> members = conversations.members(conversationId);
        if (!members.contains(userId)) {
            throw new ChatException("FORBIDDEN", "bu sohbetin üyesi değilsin");
        }
        if (members.size() > 500) {
            return;                                    // kampanya kanalında "yazıyor" anlamsız
        }
        List<Long> others = members.stream().filter(m -> m != userId).toList();
        publisher.toGateways(router.route(others, userId + ":" + deviceId).targetsByGateway(),
                Frames.of("TYPING", "conversationId", conversationId, "userId", userId));
    }
}
