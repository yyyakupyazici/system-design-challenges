package io.paramix.chat.fanout;

import java.util.List;
import java.util.Map;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.gateway.GatewayIdentity;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Gateway'ler arası "telsiz": Redis Pub/Sub. Her gateway kendi kanalını (gw:&lt;instance&gt;) dinler.
 * Ateşle-unut: dinleyen yoksa mesaj kaybolur, ve bu bilinçli bir seçim: mesaj veritabanında, istemci senkronla alır.
 *
 * <p>Kampanya kanalı için tek bir "bc" kanalı: bütün gateway'ler dinler, her biri sadece kendi yerel abonelerine
 * dağıtır. 2 milyon aboneye 2 milyon PUBLISH değil, 1 PUBLISH → 25 gateway → yerel dağıtım.
 *
 * <p><b>"0 abone" = park et.</b> PUBLISH mesajı kaç abonenin aldığını döner. Canlı sandığımız bir gateway için 0
 * dönerse o an aboneliği yok demektir: en tipik hali Redis'e yeniden bağlanırken, Lettuce aboneliği tekrar kurana
 * kadarki pencere. Bu pencere sonda zaman aşımından (3 sn) kısaysa kimse fark etmez ve mesaj canlı yoldan sessizce
 * kaybolurdu (testte yakalandı). Bunun yerine mesaj o gateway'in park kutusuna ({@code gw:parked:<instance>}, Redis
 * listesi) bırakılır; gateway kutuyu her saniye boşaltıp aynı cihaz şeritlerinden teslim eder.
 * <pre>
 *  PUBLISH gw:7 → 1   normal yol
 *  PUBLISH gw:7 → 0   RPUSH gw:parked:gw-7#a81f + LTRIM (en fazla parked-max) + PEXPIRE (lease)
 *                     gateway ≤ 1 sn içinde alır; gateway gerçekten ölmüşse kutu lease süresi sonunda kendiliğinden silinir
 * </pre>
 * Kampanya kanalı (bc) için park yok: orada 0 "hiç gateway yok" demek, aboneler zaten senkronla çeker (fan-out on read).
 *
 * <p>Cluster notu: klasik PUBLISH Redis Cluster'da her mesajı bütün düğümlere kopyalar. Üretimde ya Redis 7'nin
 * sharded pub/sub'ı (SPUBLISH) ya da pub/sub için ayrı, küçük bir Redis kullanılır (README).
 */
@Component
public class DeliveryPublisher {

    public static final String BROADCAST_CHANNEL = "bc";

    /** Tek key, tek atomik adım: ekle, sınırla, süre ver. Dönüş: kutu taştıysa 1 (en eski kayıtlar atıldı). */
    private static final RedisScript<Long> PARK = new DefaultRedisScript<>("""
            local n = redis.call('RPUSH', KEYS[1], ARGV[1])
            local max = tonumber(ARGV[2])
            if n > max then redis.call('LTRIM', KEYS[1], -max, -1) end
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            if n > max then return 1 end
            return 0""", Long.class);

    private final StringRedisTemplate redis;
    private final Json json;
    private final MeterRegistry meters;
    private final ChatProperties props;

    public DeliveryPublisher(StringRedisTemplate redis, Json json, MeterRegistry meters, ChatProperties props) {
        this.redis = redis;
        this.json = json;
        this.meters = meters;
        this.props = props;
    }

    public void toGateways(Map<String, List<String>> targetsByGateway, Map<String, Object> frame) {
        targetsByGateway.forEach((gateway, targets) -> {
            String payload = json.write(Map.of("targets", targets, "frame", frame));
            Long receivers = redis.convertAndSend(GatewayIdentity.channelOf(gateway), payload);
            if (receivers != null && receivers == 0) {
                Long overflow = redis.execute(PARK, List.of(GatewayIdentity.parkedKeyOf(gateway)), payload,
                        String.valueOf(props.gateway().parkedMax()), String.valueOf(props.gateway().lease().toMillis()));
                meters.counter("chat.delivery.parked").increment(targets.size());
                if (overflow != null && overflow == 1) {
                    meters.counter("chat.delivery.parked_overflow").increment();
                }
                return;
            }
            meters.counter("chat.delivery.published").increment(targets.size());
        });
    }

    public void broadcast(long channelId, Map<String, Object> frame) {
        redis.convertAndSend(BROADCAST_CHANNEL, json.write(Map.of("channelId", channelId, "frame", frame)));
    }
}
