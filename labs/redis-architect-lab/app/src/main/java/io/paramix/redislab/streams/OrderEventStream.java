package io.paramix.redislab.streams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Redis Streams: Kafka'ya en yakın Redis yapısı. Pub/Sub'dan farkı: mesajlar KALIR, tüketici grupları kendi
 * yerini tutar, işlenip onaylanmayan mesajlar (pending) başka tüketiciye devredilebilir.
 *
 * <pre>
 *  XADD          → olay ekle (MAXLEN ~ ile boyut sınırlı: RAM sınırsız değil)
 *  XREADGROUP    → grup içinde her mesaj tek tüketiciye gider (Kafka consumer group gibi)
 *  XACK          → "işledim"
 *  XPENDING      → alınmış ama onaylanmamış mesajlar (tüketici öldü mü?)
 *  XCLAIM        → takılan mesajı başka tüketiciye devret
 *  N kez denendi → DLQ stream'ine taşı ve onayla (zehirli mesaj kuyruğu tıkamasın)
 * </pre>
 *
 * Kafka'dan farkı: tek bir stream tek bir slotta durur (bir düğümün RAM'i ve CPU'su), partition yok.
 * Yüksek hacimde birden çok stream'e bölmek senin işin. Uzun saklama ve replay için Kafka daha uygun.
 */
@Service
public class OrderEventStream {

    private final StringRedisTemplate redis;

    public OrderEventStream(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String stream(String name) {
        return "stream:{" + name + "}";
    }

    public static String dlq(String name) {
        return "stream:{" + name + "}:dlq";
    }

    public RecordId publish(String name, Map<String, String> event) {
        return redis.opsForStream().add(MapRecord.create(stream(name), event),
                XAddOptions.maxlen(100_000).approximateTrimming(true));
    }

    /** Grup yoksa oluşturur (stream de yoksa MKSTREAM ile). Zaten varsa BUSYGROUP hatası yok sayılır. */
    public void ensureGroup(String name, String group) {
        try {
            redis.execute((org.springframework.data.redis.core.RedisCallback<Object>) c ->
                    c.streamCommands().xGroupCreate(stream(name).getBytes(), group, ReadOffset.from("0"), true));
        } catch (RedisSystemException e) {
            // Redis'in asıl mesajı Spring'in sardığı exception'ın kök nedeninde
            String msg = org.springframework.core.NestedExceptionUtils.getMostSpecificCause(e).getMessage();
            if (msg == null || !msg.contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    public List<MapRecord<String, Object, Object>> read(String name, String group, String consumer, int count) {
        List<MapRecord<String, Object, Object>> r = redis.opsForStream().read(Consumer.from(group, consumer),
                StreamReadOptions.empty().count(count), StreamOffset.create(stream(name), ReadOffset.lastConsumed()));
        return r == null ? List.of() : r;
    }

    public void ack(String name, String group, RecordId id) {
        redis.opsForStream().acknowledge(stream(name), group, id);
    }

    public PendingMessages pending(String name, String group, int count) {
        return redis.opsForStream().pending(stream(name), group, Range.unbounded(), count);
    }

    /**
     * Takılan mesajları kurtarır: en az {@code minIdle} süredir onaylanmamış mesajları {@code consumer}'a devreder.
     * {@code maxDeliveries} kez denenmiş olanlar DLQ'ya gider. Dönüş: devralınan mesajlar.
     */
    public List<MapRecord<String, Object, Object>> recover(String name, String group, String consumer,
                                                          Duration minIdle, int maxDeliveries) {
        List<RecordId> claimable = new ArrayList<>();
        for (PendingMessage p : pending(name, group, 100)) {
            if (p.getElapsedTimeSinceLastDelivery().compareTo(minIdle) < 0) {
                continue;
            }
            if (p.getTotalDeliveryCount() >= maxDeliveries) {
                moveToDlq(name, group, p.getId());
            } else {
                claimable.add(p.getId());
            }
        }
        if (claimable.isEmpty()) {
            return List.of();
        }
        return redis.opsForStream().claim(stream(name), group, consumer, minIdle, claimable.toArray(RecordId[]::new));
    }

    private void moveToDlq(String name, String group, RecordId id) {
        List<MapRecord<String, Object, Object>> rec = redis.opsForStream().range(stream(name), Range.closed(id.getValue(), id.getValue()));
        if (rec != null && !rec.isEmpty()) {
            Map<Object, Object> body = new java.util.LinkedHashMap<>(rec.getFirst().getValue());
            body.put("originalId", id.getValue());
            redis.opsForStream().add(MapRecord.create(dlq(name), body));
        }
        redis.opsForStream().acknowledge(stream(name), group, id);
    }

    public long dlqSize(String name) {
        Long n = redis.opsForStream().size(dlq(name));
        return n == null ? 0 : n;
    }
}
