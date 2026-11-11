package io.paramix.payreq;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Bu servisin kendi veritabanı. Docker varsa Testcontainers; yoksa hazır PostgreSQL:
 * IT_PAYREQ_PG_URL (yoksa IT_PG_URL'deki veritabanı adı "payreq" yapılır), IT_PG_USER, IT_PG_PASSWORD.
 */
public final class PayReqTestInfra {

    private static PostgreSQLContainer<?> postgres;

    private PayReqTestInfra() {
    }

    public static boolean available() {
        if (System.getenv("IT_PG_URL") != null || System.getenv("IT_PAYREQ_PG_URL") != null) {
            return true;
        }
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    private static boolean external() {
        return System.getenv("IT_PG_URL") != null || System.getenv("IT_PAYREQ_PG_URL") != null;
    }

    public static synchronized String url() {
        if (System.getenv("IT_PAYREQ_PG_URL") != null) {
            return System.getenv("IT_PAYREQ_PG_URL");
        }
        if (System.getenv("IT_PG_URL") != null) {
            return System.getenv("IT_PG_URL").replaceFirst("/[^/?]+(\\?|$)", "/payreq$1");
        }
        return postgres().getJdbcUrl();
    }

    public static synchronized String user() {
        return external() ? System.getenv().getOrDefault("IT_PG_USER", "postgres") : postgres().getUsername();
    }

    public static synchronized String password() {
        return external() ? System.getenv().getOrDefault("IT_PG_PASSWORD", "") : postgres().getPassword();
    }

    private static PostgreSQLContainer<?> postgres() {
        if (postgres == null) {
            postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("payreq").withUsername("payreq").withPassword("payreq");
            postgres.start();
        }
        return postgres;
    }
}
