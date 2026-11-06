package io.paramix.redislab.ratelimit;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Üç rate limit algoritması, üç farklı Redis veri yapısı:
 *
 * <table>
 *   <tr><th>Algoritma</th><th>Yapı</th><th>Bellek</th><th>Doğruluk</th><th>Ne zaman</th></tr>
 *   <tr><td>Sabit pencere</td><td>String + INCR</td><td>istemci başı 1 sayı</td><td>Pencere sınırında 2× patlama</td>
 *       <td>Kaba koruma, çok sayıda istemci</td></tr>
 *   <tr><td>Kayan pencere (log)</td><td>ZSET</td><td>istemci başı limit kadar eleman</td><td>Tam</td>
 *       <td>Düşük limitler (login denemesi, OTP)</td></tr>
 *   <tr><td>Token bucket</td><td>Hash</td><td>istemci başı 2 alan</td><td>Patlamaya kontrollü izin</td>
 *       <td>API limitleri (001'deki tasarım)</td></tr>
 * </table>
 */
@Service
public class RateLimiters {

    private static final RedisScript<List<Object>> FIXED = Scripts.loadList("rl_fixed_window");
    private static final RedisScript<List<Object>> SLIDING = Scripts.loadList("rl_sliding_log");
    private static final RedisScript<List<Object>> BUCKET = Scripts.loadList("rl_token_bucket");

    private final StringRedisTemplate redis;

    public RateLimiters(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Decision fixedWindow(String client, long limit, Duration window) {
        long w = System.currentTimeMillis() / window.toMillis();
        return decide(redis.execute(FIXED, List.of("rl:fw:{" + client + "}:" + w),
                String.valueOf(limit), String.valueOf(window.toMillis())));
    }

    public Decision slidingLog(String client, long limit, Duration window) {
        return decide(redis.execute(SLIDING, List.of("rl:sw:{" + client + "}"),
                String.valueOf(limit), String.valueOf(window.toMillis()), UUID.randomUUID().toString()));
    }

    public Decision tokenBucket(String client, long capacity, double tokensPerSecond) {
        return decide(redis.execute(BUCKET, List.of("rl:tb:{" + client + "}"),
                String.valueOf(capacity), String.valueOf(tokensPerSecond), "1"));
    }

    private static Decision decide(List<Object> r) {
        return new Decision(((Number) r.get(0)).longValue() == 1L, ((Number) r.get(1)).longValue());
    }
}
