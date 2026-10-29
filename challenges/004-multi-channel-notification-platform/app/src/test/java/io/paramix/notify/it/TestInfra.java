package io.paramix.notify.it;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Entegrasyon testleri için PostgreSQL + Redis. Kafka her zaman gömülü (EmbeddedKafka).
 *
 * <p>Varsayılan: Testcontainers. Docker yoksa hazır servisler env ile verilebilir:
 * IT_REDIS_HOST, IT_REDIS_PORT, IT_PG_URL, IT_PG_USER, IT_PG_PASSWORD. İkisi de yoksa IT'ler atlanır.
 */
public final class TestInfra {

    private static GenericContainer<?> redis;
    private static PostgreSQLContainer<?> postgres;

    private TestInfra() {
    }

    /** JUnit @EnabledIf için. */
    public static boolean available() {
        if (System.getenv("IT_REDIS_HOST") != null && System.getenv("IT_PG_URL") != null) {
            return true;
        }
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    public static boolean redisAvailable() {
        return System.getenv("IT_REDIS_HOST") != null || available();
    }

    public static synchronized String redisHost() {
        String host = System.getenv("IT_REDIS_HOST");
        return host != null ? host : redis().getHost();
    }

    public static synchronized int redisPort() {
        if (System.getenv("IT_REDIS_HOST") != null) {
            return Integer.parseInt(System.getenv().getOrDefault("IT_REDIS_PORT", "6379"));
        }
        return redis().getMappedPort(6379);
    }

    public static synchronized String pgUrl() {
        String url = System.getenv("IT_PG_URL");
        return url != null ? url : postgres().getJdbcUrl();
    }

    public static synchronized String pgUser() {
        return System.getenv("IT_PG_URL") != null ? System.getenv().getOrDefault("IT_PG_USER", "postgres") : postgres().getUsername();
    }

    public static synchronized String pgPassword() {
        return System.getenv("IT_PG_URL") != null ? System.getenv().getOrDefault("IT_PG_PASSWORD", "") : postgres().getPassword();
    }

    private static GenericContainer<?> redis() {
        if (redis == null) {
            redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
            redis.start();
        }
        return redis;
    }

    private static PostgreSQLContainer<?> postgres() {
        if (postgres == null) {
            postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("notify").withUsername("notify").withPassword("notify");
            postgres.start();
        }
        return postgres;
    }
}
