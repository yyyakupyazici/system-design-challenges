package io.paramix.chat.it;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * PostgreSQL + Redis. Kafka her zaman gömülü (EmbeddedKafkaKraftBroker).
 * Docker varsa Testcontainers; yoksa hazır servisler: IT_PG_URL, IT_PG_USER, IT_PG_PASSWORD, IT_REDIS_HOST, IT_REDIS_PORT.
 */
public final class TestInfra {

    private static GenericContainer<?> redis;
    private static PostgreSQLContainer<?> postgres;

    private TestInfra() {
    }

    public static boolean available() {
        if (System.getenv("IT_PG_URL") != null && System.getenv("IT_REDIS_HOST") != null) {
            return true;
        }
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    public static synchronized String redisHost() {
        return System.getenv("IT_REDIS_HOST") != null ? System.getenv("IT_REDIS_HOST") : redis().getHost();
    }

    public static synchronized int redisPort() {
        return System.getenv("IT_REDIS_HOST") != null
                ? Integer.parseInt(System.getenv().getOrDefault("IT_REDIS_PORT", "6379"))
                : redis().getMappedPort(6379);
    }

    public static synchronized String pgUrl() {
        return System.getenv("IT_PG_URL") != null ? System.getenv("IT_PG_URL") : postgres().getJdbcUrl();
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
                    .withDatabaseName("chat").withUsername("chat").withPassword("chat");
            postgres.start();
        }
        return postgres;
    }
}
