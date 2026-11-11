package io.paramix.chat.push;

import java.util.List;

import io.paramix.chat.fanout.DeliveryRouter;
import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.outbox.ChatEvent;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Çevrimdışı alıcılara bildirim, canlı tesliminden AYRI bir consumer group'ta ("chat-push"). Aynı Kafka olaylarını
 * kendi hızında, kendi offset'iyle okur. Kafka'yı seçme sebebimiz olan "çok bağımsız tüketici" tam olarak bu.
 *
 * <p>Redis'e ulaşılamazsa kimin çevrimiçi olduğunu bilemeyiz. Şüphede HERKESE (gönderen hariç) bildirim gönderilir:
 * çevrimiçi biri fazladan bir bildirim görür, ama kimse mesajı kaçırmaz.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.fanout", havingValue = "true", matchIfMissing = true)
public class PushWorker {

    private static final Logger log = LoggerFactory.getLogger(PushWorker.class);

    private final Json json;
    private final ConversationRepository conversations;
    private final DeliveryRouter router;
    private final PushSender push;
    private final MeterRegistry meters;

    public PushWorker(Json json, ConversationRepository conversations, DeliveryRouter router, PushSender push,
                      MeterRegistry meters) {
        this.json = json;
        this.conversations = conversations;
        this.router = router;
        this.push = push;
        this.meters = meters;
    }

    @KafkaListener(topics = "${chat.kafka.topic}", groupId = "chat-push", concurrency = "${chat.fanout.concurrency:3}")
    public void onEvent(String payload) throws Exception {
        ChatEvent e = json.mapper().readValue(payload, ChatEvent.class);
        if (e.pushPreview() == null || "BROADCAST".equals(e.conversationKind())) {
            return;                                           // okundu bilgisi, kampanya kanalı: bildirim yok
        }
        List<Long> members = conversations.members(e.conversationId());
        List<Long> targets;
        try {
            targets = router.route(members, null).offlineUsers();
        } catch (DataAccessException ex) {
            log.warn("Rehbere ulaşılamıyor, sohbet {} için bütün üyelere bildirim gidiyor: {}", e.conversationId(), ex.getMessage());
            meters.counter("chat.push.fallback_all").increment();
            targets = members;
        }
        for (Long u : targets) {
            if (u != e.actorUserId()) {
                push.send(u, e.conversationId(), e.pushPreview());
            }
        }
    }
}
