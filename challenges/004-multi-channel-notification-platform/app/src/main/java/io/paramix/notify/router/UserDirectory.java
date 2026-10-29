package io.paramix.notify.router;

import java.time.Duration;
import java.util.Optional;

import io.paramix.notify.store.UserProfile;
import io.paramix.notify.store.UserRepository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Kullanıcı profili için cache-aside (Redis).
 * Değişiklikte cache'e yeni değer YAZILMAZ, key SİLİNİR: olaylar hangi sırayla gelirse gelsin
 * bir sonraki okuma DB'den taze veriyi getirir. TTL, silme kaybolursa son emniyet ağıdır.
 */
@Component
public class UserDirectory {

    private static final Logger log = LoggerFactory.getLogger(UserDirectory.class);

    private final UserRepository users;
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final Duration ttl;

    public UserDirectory(UserRepository users, StringRedisTemplate redis, ObjectMapper json,
                         @Value("${notify.users.cache-ttl:10m}") Duration ttl) {
        this.users = users;
        this.redis = redis;
        this.json = json;
        this.ttl = ttl;
    }

    public Optional<UserProfile> get(String userId) {
        String key = key(userId);
        try {
            String cached = redis.opsForValue().get(key);
            if (cached != null) {
                return Optional.of(json.readValue(cached, UserProfile.class));
            }
        } catch (Exception e) {
            log.debug("Kullanıcı cache'i okunamadı, DB'ye gidiliyor: {}", e.getMessage());
        }
        Optional<UserProfile> loaded = users.load(userId);
        loaded.ifPresent(p -> {
            try {
                redis.opsForValue().set(key, json.writeValueAsString(p), ttl);
            } catch (Exception e) {
                log.debug("Kullanıcı cache'e yazılamadı: {}", e.getMessage());
            }
        });
        return loaded;
    }

    public void invalidate(String userId) {
        try {
            redis.delete(key(userId));
        } catch (Exception e) {
            log.warn("Kullanıcı cache'i silinemedi ({}), TTL dolunca düzelecek: {}", userId, e.getMessage());
        }
    }

    private static String key(String userId) {
        return "user:" + userId + ":profile";
    }
}
