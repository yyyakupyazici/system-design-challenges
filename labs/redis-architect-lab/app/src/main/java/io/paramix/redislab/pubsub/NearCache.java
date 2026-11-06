package io.paramix.redislab.pubsub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Pub/Sub ile yerel cache (near cache) geçersiz kılma. Her pod bir veriyi kendi belleğinde tutar (Redis'e bile
 * gitmeden nanosaniyede okur). Veri değişince bir pod "şu key değişti" yayınlar, bütün pod'lar yerel kopyayı siler.
 *
 * <p><b>Pub/Sub ateşle-unut'tur:</b> mesaj o an dinlemeyen pod'a hiç ulaşmaz, saklanmaz, tekrar gönderilmez.
 * Bu yüzden sadece "kaybolursa en kötü ihtimalle bir süre eski veri görülür" durumlarında kullanılır, ve yerel
 * cache'e her zaman kısa bir TTL de konur (kaybolan mesajın etkisi sınırlı kalsın). Kaybolmaması gereken olay
 * için Streams ({@link io.paramix.redislab.streams.OrderEventStream}) kullanılır.
 */
@Component
public class NearCache implements MessageListener {

    public static final String CHANNEL = "cache-invalidation";

    private final Map<String, String> local = new ConcurrentHashMap<>();
    private final StringRedisTemplate redis;

    public NearCache(StringRedisTemplate redis, RedisMessageListenerContainer container) {
        this.redis = redis;
        container.addMessageListener(this, new ChannelTopic(CHANNEL));
    }

    public void putLocal(String key, String value) {
        local.put(key, value);
    }

    public String getLocal(String key) {
        return local.get(key);
    }

    /** Veri değişti: bütün pod'lara haber ver. Dönüş: mesajı alan abone sayısı (0 = kimse dinlemiyordu). */
    public long invalidateEverywhere(String key) {
        Long n = redis.convertAndSend(CHANNEL, key);
        return n == null ? 0 : n;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        local.remove(new String(message.getBody()));
    }
}
