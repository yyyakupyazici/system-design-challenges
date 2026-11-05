package io.paramix.llmgw.limit;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Token/dakika limiti. İki yerde kullanılır:
 * <ul>
 *   <li>Ekip başına: deneysel ekibin toplu işi kendi kovasını boşaltır, destek asistanınınkine dokunamaz (zorluk #6).</li>
 *   <li>Sağlayıcı başına: yedek sağlayıcının kapasitesi birincilinkinin dörtte biriyse, fallback trafiği onu da
 *       çökertmesin; kova doluysa o yedek atlanır (zorluk #2).</li>
 * </ul>
 * Tahmini token düşülür, mutabakatta fark iade edilir.
 */
@Component
public class TokenRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenRateLimiter.class);

    public record Decision(boolean allowed, long waitMs) {
    }

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> script;

    public TokenRateLimiter(StringRedisTemplate redis) {
        this.redis = redis;
        this.script = new DefaultRedisScript<>();
        this.script.setLocation(new ClassPathResource("scripts/token_bucket.lua"));
        this.script.setResultType(List.class);
    }

    public Decision tryConsume(String bucket, long tokensPerMinute, long tokens) {
        try {
            List<?> r = redis.execute(script, List.of("tpm:{" + bucket + "}"),
                    String.valueOf(tokensPerMinute), String.valueOf(tokens), "120000");
            return new Decision(((Number) r.get(0)).longValue() == 1, ((Number) r.get(1)).longValue());
        } catch (DataAccessException e) {
            // Redis yoksa hız limiti uygulanamaz: isteği geçir (bütçe rezervasyonu zaten ayrı bir koruma).
            log.warn("Token limiti kontrol edilemedi ({}): {}", bucket, e.getMessage());
            return new Decision(true, 0);
        }
    }

    public void refund(String bucket, long tokensPerMinute, long tokens) {
        if (tokens <= 0) {
            return;
        }
        try {
            redis.execute(script, List.of("tpm:{" + bucket + "}"), String.valueOf(tokensPerMinute),
                    String.valueOf(-tokens), "120000");
        } catch (DataAccessException e) {
            log.debug("Token iadesi yapılamadı: {}", e.getMessage());
        }
    }
}
