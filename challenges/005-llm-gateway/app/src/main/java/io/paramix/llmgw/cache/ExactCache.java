package io.paramix.llmgw.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import io.paramix.llmgw.api.ChatCompletionRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Birebir aynı istek → aynı cevap. Anahtar ekip + uygulama ile kapsamlanır (ekipler arası sızıntı yok),
 * içerik model + parametreler + mesajların özetidir. Redis yoksa cache yok sayılır (fail-open: sadece pahalıya patlar).
 */
@Component
public class ExactCache {

    private static final Logger log = LoggerFactory.getLogger(ExactCache.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public ExactCache(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    public static String key(String team, String app, String modelAlias, String modelVersion, Integer maxTokens,
                             Double temperature, List<ChatCompletionRequest.Message> maskedMessages) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((modelAlias + "|" + modelVersion + "|" + maxTokens + "|" + temperature).getBytes(StandardCharsets.UTF_8));
            for (ChatCompletionRequest.Message m : maskedMessages) {
                md.update(("\u0000" + m.role() + "\u0001" + m.content()).getBytes(StandardCharsets.UTF_8));
            }
            return "c:x:" + team + ":" + app + ":" + HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Optional<CachedAnswer> get(String key) {
        try {
            String v = redis.opsForValue().get(key);
            return v == null ? Optional.empty() : Optional.of(json.readValue(v, CachedAnswer.class));
        } catch (Exception e) {
            log.debug("Exact cache okunamadı: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public void put(String key, CachedAnswer answer, Duration ttl) {
        try {
            redis.opsForValue().set(key, json.writeValueAsString(answer), ttl);
        } catch (Exception e) {
            log.debug("Exact cache yazılamadı: {}", e.getMessage());
        }
    }
}
