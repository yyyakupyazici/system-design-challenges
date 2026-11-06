package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import io.paramix.redislab.presence.PresenceService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

class PresenceIT extends AbstractRedisIT {

    @Autowired PresenceService presence;
    @Autowired StringRedisTemplate redis;

    @Test
    void sessionExpiresUnlessTouched() throws Exception {
        String a = unique("sess");
        String b = unique("sess");
        presence.createSession(a, Map.of("userId", "u1"), Duration.ofSeconds(2));
        presence.createSession(b, Map.of("userId", "u2"), Duration.ofSeconds(2));
        for (int i = 0; i < 3; i++) {
            Thread.sleep(1000);
            presence.touchSession(a, Duration.ofSeconds(2));    // sadece a aktif
        }
        assertThat(presence.touchSession(a, Duration.ofSeconds(2))).containsEntry("userId", "u1");
        assertThat(presence.touchSession(b, Duration.ofSeconds(2))).isEmpty();   // b süresi doldu
    }

    @Test
    void onlineCountOnlyIncludesRecentHeartbeats() {
        String room = unique("oda");
        presence.heartbeat(room, "u1");
        presence.heartbeat(room, "u2");
        redis.opsForZSet().add("presence:{" + room + "}", "u3", System.currentTimeMillis() - 120_000);   // 2 dk önce
        assertThat(presence.onlineCount(room, Duration.ofSeconds(60))).isEqualTo(2);
        assertThat(presence.prune(room, Duration.ofSeconds(60))).isEqualTo(1);
    }
}
