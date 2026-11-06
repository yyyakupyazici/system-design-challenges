package io.paramix.redislab.it;

import java.util.UUID;

import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Bütün IT'ler aynı Spring context'ini paylaşır. Testler birbirini bozmasın diye her test kendi benzersiz
 * key'lerini kullanır ({@link #unique}); cluster'da FLUSHALL her master'da ayrı çalışmak zorunda kalırdı.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIf("io.paramix.redislab.it.TestRedis#available")
public abstract class AbstractRedisIT {

    @DynamicPropertySource
    static void redis(DynamicPropertyRegistry r) {
        if (TestRedis.isCluster()) {
            r.add("spring.data.redis.cluster.nodes", TestRedis::clusterNodes);
            r.add("spring.data.redis.lettuce.cluster.refresh.adaptive", () -> "true");
        } else {
            r.add("spring.data.redis.host", TestRedis::host);
            r.add("spring.data.redis.port", TestRedis::port);
        }
    }

    protected static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
