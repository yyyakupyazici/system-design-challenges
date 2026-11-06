package io.paramix.redislab.common;

import java.util.List;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Lua script'lerini classpath'ten yükler. Spring her çağrıda önce EVALSHA dener (sadece 40 karakterlik SHA gider),
 * script sunucuda yoksa EVAL ile gövdeyi gönderir. Script'i bir kez yükleyip her yerde aynı nesneyi kullanmak
 * SHA'nın bir kez hesaplanmasını sağlar.
 */
public final class Scripts {

    private Scripts() {
    }

    public static <T> RedisScript<T> load(String name, Class<T> resultType) {
        DefaultRedisScript<T> s = new DefaultRedisScript<>();
        s.setLocation(new ClassPathResource("scripts/" + name + ".lua"));
        s.setResultType(resultType);
        return s;
    }

    @SuppressWarnings("unchecked")
    public static RedisScript<List<Object>> loadList(String name) {
        return (RedisScript<List<Object>>) (RedisScript<?>) load(name, List.class);
    }
}
