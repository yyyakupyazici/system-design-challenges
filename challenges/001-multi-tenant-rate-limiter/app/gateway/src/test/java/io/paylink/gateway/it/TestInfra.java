package io.paylink.gateway.it;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Entegrasyon testleri için Redis + PostgreSQL.
 *
 * <p>Varsayılan: Testcontainers. Docker yoksa (ör. CI'da hazır servisler varsa) şu env değişkenleriyle
 * dış servis kullanılabilir: IT_REDIS_HOST, IT_REDIS_PORT, IT_PG_URL, IT_PG_USER, IT_PG_PASSWORD.
 */
public final class TestInfra {

    private static GenericContainer<?> redis;
    private static PostgreSQLContainer<?> postgres;

    private TestInfra() {
    }

    /** JUnit @EnabledIf için. */
    public static boolean available() {
        if (System.getenv("IT_REDIS_HOST") != null) {
            return true;
        }
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    public static synchronized String redisHost() {
        String host = System.getenv("IT_REDIS_HOST");
        if (host != null) {
            return host;
        }
        return redis().getHost();
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
            postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));
            postgres.start();
        }
        return postgres;
    }
}
