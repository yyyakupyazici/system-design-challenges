package io.paramix.redislab.delayedqueue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Gecikmeli ve güvenilir iş kuyruğu (ZSET ile). "30 dk sonra sepet hatırlatması gönder", "rezervasyon 15 dk'da
 * ödenmezse iptal et", 005'teki bütçe reaper'ı.
 *
 * <pre>
 *  ready       ZSET  skor = çalışma zamanı        schedule → buraya
 *  processing  ZSET  skor = görünürlük bitişi     claim → ready'den buraya taşınır (silinmez!)
 *  jobs        HASH  id → payload                  ack → hepsinden silinir
 *  reaper: processing'te süresi dolan (worker'ı ölmüş) işleri ready'ye geri alır → iş asla kaybolmaz
 * </pre>
 *
 * Teslim garantisi "en az bir kez": worker işi yapıp ack'leyemeden ölürse iş tekrar verilir. İşler idempotent olmalı.
 * Bu, SQS'in "visibility timeout" modelinin aynısı.
 */
@Service
public class DelayedJobQueue {

    public record Job(String id, String payload) {
    }

    private static final RedisScript<Long> SCHEDULE = Scripts.load("dq_schedule", Long.class);
    private static final RedisScript<List<Object>> CLAIM = Scripts.loadList("dq_claim");
    private static final RedisScript<Long> REQUEUE = Scripts.load("dq_requeue", Long.class);

    private final StringRedisTemplate redis;

    public DelayedJobQueue(StringRedisTemplate redis) {
        this.redis = redis;
    }

    static String ready(String q) {
        return "dq:{" + q + "}:ready";
    }

    static String processing(String q) {
        return "dq:{" + q + "}:processing";
    }

    static String jobs(String q) {
        return "dq:{" + q + "}:jobs";
    }

    public void schedule(String queue, String id, String payload, Instant runAt) {
        redis.execute(SCHEDULE, List.of(ready(queue), jobs(queue)), id, payload, String.valueOf(runAt.toEpochMilli()));
    }

    public List<Job> claim(String queue, int max, long visibilityMs) {
        List<Object> r = redis.execute(CLAIM, List.of(ready(queue), processing(queue), jobs(queue)),
                String.valueOf(max), String.valueOf(visibilityMs));
        List<Job> out = new ArrayList<>();
        for (int i = 0; r != null && i + 1 < r.size(); i += 2) {
            out.add(new Job(r.get(i).toString(), r.get(i + 1).toString()));
        }
        return out;
    }

    public void ack(String queue, String id) {
        redis.opsForZSet().remove(processing(queue), id);
        redis.opsForHash().delete(jobs(queue), id);
    }

    /** Reaper. Gerçek sistemde @Scheduled ile birkaç saniyede bir çalışır. */
    public long requeueExpired(String queue) {
        Long r = redis.execute(REQUEUE, List.of(ready(queue), processing(queue)));
        return r == null ? 0 : r;
    }

    public long readyCount(String queue) {
        Long n = redis.opsForZSet().zCard(ready(queue));
        return n == null ? 0 : n;
    }
}
