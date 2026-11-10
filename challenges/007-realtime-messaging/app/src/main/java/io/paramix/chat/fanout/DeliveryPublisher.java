package io.paramix.chat.fanout;

import java.util.List;
import java.util.Map;

import io.paramix.chat.gateway.GatewayIdentity;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Gateway'ler arası "telsiz": Redis Pub/Sub. Her gateway kendi kanalını (gw:&lt;instance&gt;) dinler.
 * Ateşle-unut: dinleyen yoksa mesaj kaybolur, ve bu bilinçli bir seçim: mesaj veritabanında, istemci senkronla alır.
 *
 * <p>Kampanya kanalı için tek bir "bc" kanalı: bütün gateway'ler dinler, her biri sadece kendi yerel abonelerine
 * dağıtır. 2 milyon aboneye 2 milyon PUBLISH değil, 1 PUBLISH → 25 gateway → yerel dağıtım.
 *
 * <p>Cluster notu: klasik PUBLISH Redis Cluster'da her mesajı bütün düğümlere kopyalar. Üretimde ya Redis 7'nin
 * sharded pub/sub'ı (SPUBLISH) ya da pub/sub için ayrı, küçük bir Redis kullanılır (README).
 */
@Component
public class DeliveryPublisher {

    public static final String BROADCAST_CHANNEL = "bc";

    private final StringRedisTemplate redis;
    private final Json json;
    private final MeterRegistry meters;

    public DeliveryPublisher(StringRedisTemplate redis, Json json, MeterRegistry meters) {
        this.redis = redis;
        this.json = json;
        this.meters = meters;
    }

    public void toGateways(Map<String, List<String>> targetsByGateway, Map<String, Object> frame) {
        targetsByGateway.forEach((gateway, targets) -> {
            redis.convertAndSend(GatewayIdentity.channelOf(gateway), json.write(Map.of("targets", targets, "frame", frame)));
            meters.counter("chat.delivery.published").increment(targets.size());
        });
    }

    public void broadcast(long channelId, Map<String, Object> frame) {
        redis.convertAndSend(BROADCAST_CHANNEL, json.write(Map.of("channelId", channelId, "frame", frame)));
    }
}
