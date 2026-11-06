package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import io.paramix.redislab.pubsub.NearCache;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class PubSubIT extends AbstractRedisIT {

    @Autowired NearCache near;

    @Test
    void invalidationReachesTheLocalCache() {
        String key = unique("config");
        near.putLocal(key, "eski");
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            near.invalidateEverywhere(key);                     // abonelik hazır olana kadar tekrar dene
            return near.getLocal(key) == null;
        });
    }
}
