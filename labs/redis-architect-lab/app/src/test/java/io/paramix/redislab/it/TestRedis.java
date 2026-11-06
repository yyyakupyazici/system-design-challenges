package io.paramix.redislab.it;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testler hangi Redis'e bağlanacak? Sırayla:
 * <ol>
 *   <li>{@code IT_REDIS_CLUSTER=127.0.0.1:7000,127.0.0.1:7001,...} → gerçek cluster (scripts/local-cluster.sh)</li>
 *   <li>{@code IT_REDIS_HOST} / {@code IT_REDIS_PORT} → tek Redis</li>
 *   <li>Docker varsa Testcontainers ile tek Redis</li>
 * </ol>
 * Hiçbiri yoksa IT'ler atlanır. Cluster'a özgü testler sadece 1. durumda çalışır.
 */
public final class TestRedis {

    private static GenericContainer<?> container;

    private TestRedis() {
    }

    public static String clusterNodes() {
        return System.getenv("IT_REDIS_CLUSTER");
    }

    public static boolean isCluster() {
        return clusterNodes() != null && !clusterNodes().isBlank();
    }

    public static boolean available() {
        if (isCluster() || System.getenv("IT_REDIS_HOST") != null) {
            return true;
        }
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    public static synchronized String host() {
        String h = System.getenv("IT_REDIS_HOST");
        return h != null ? h : container().getHost();
    }

    public static synchronized int port() {
        if (System.getenv("IT_REDIS_HOST") != null) {
            return Integer.parseInt(System.getenv().getOrDefault("IT_REDIS_PORT", "6379"));
        }
        return container().getMappedPort(6379);
    }

    private static GenericContainer<?> container() {
        if (container == null) {
            container = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
            container.start();
        }
        return container;
    }
}
