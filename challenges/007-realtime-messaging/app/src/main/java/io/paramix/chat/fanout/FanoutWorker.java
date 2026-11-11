package io.paramix.chat.fanout;

import java.util.List;

import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.outbox.ChatEvent;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka'dan gelen her olayı alıcılara dağıtır. Kafka key = sohbet id → bir sohbetin olayları sırayla, tek tüketicide.
 *
 * <pre>
 *  DIRECT / GROUP (≤ 500 üye)  FAN-OUT ON WRITE: üyelerin canlı cihazlarını bul → gateway başına tek PUBLISH.
 *                              Push bu worker'da DEĞİL: ayrı consumer group ({@link io.paramix.chat.push.PushWorker}).
 *                              Redis çökünce canlı teslim durur ama bildirimler durmamalı.
 *  BROADCAST (2 milyon abone)  Üye listesi DOLAŞILMAZ: tek PUBLISH "bc" → her gateway kendi yerel abonelerine verir.
 *                              Çevrimdışı abonelere push yok; uygulamayı açınca senkronla çekerler (fan-out on read).
 * </pre>
 */
@Component
@ConditionalOnProperty(name = "chat.roles.fanout", havingValue = "true", matchIfMissing = true)
public class FanoutWorker {

    private final Json json;
    private final ConversationRepository conversations;
    private final DeliveryRouter router;
    private final DeliveryPublisher publisher;
    private final MeterRegistry meters;

    public FanoutWorker(Json json, ConversationRepository conversations, DeliveryRouter router,
                        DeliveryPublisher publisher, MeterRegistry meters) {
        this.json = json;
        this.conversations = conversations;
        this.router = router;
        this.publisher = publisher;
        this.meters = meters;
    }

    @KafkaListener(topics = "${chat.kafka.topic}", concurrency = "${chat.fanout.concurrency:3}")
    public void onEvent(String payload) throws Exception {
        ChatEvent e = json.mapper().readValue(payload, ChatEvent.class);
        if ("BROADCAST".equals(e.conversationKind())) {
            publisher.broadcast(e.conversationId(), e.frame());
            meters.counter("chat.fanout", "kind", "broadcast").increment();
            return;
        }
        List<Long> members = conversations.members(e.conversationId());
        // Redis'e ulaşılamazsa exception → KafkaConfig'teki beklemeli tekrar deneme
        DeliveryRouter.Route route = router.route(members, e.excludeDevice());
        publisher.toGateways(route.targetsByGateway(), e.frame());
        meters.counter("chat.fanout", "kind", e.conversationKind().toLowerCase()).increment();
    }
}
