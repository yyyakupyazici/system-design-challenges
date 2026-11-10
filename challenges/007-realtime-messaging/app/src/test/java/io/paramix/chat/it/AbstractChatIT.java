package io.paramix.chat.it;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;

@EnabledIf("io.paramix.chat.it.TestInfra#available")
public abstract class AbstractChatIT {

    protected static ChatCluster cluster;
    protected static int portA;
    protected static int portB;
    private static final ObjectMapper JSON = new ObjectMapper();
    private final List<TestClient> clients = new ArrayList<>();

    @BeforeAll
    static void startCluster() {
        cluster = ChatCluster.get();
        portA = ChatCluster.port(cluster.nodeA);
        portB = ChatCluster.port(cluster.nodeB);
    }

    @AfterEach
    void closeClients() {
        clients.forEach(c -> {
            try {
                c.close();
            } catch (Exception ignored) {
                // zaten kapanmış
            }
        });
    }

    protected TestClient connect(int port, long user, String device) {
        TestClient c = TestClient.connect(port, user, device);
        clients.add(c);
        return c;
    }

    protected static long id() {
        return ThreadLocalRandom.current().nextLong(1_000_000_000L, Long.MAX_VALUE / 4);
    }

    protected static long user(String name) {
        return user(name, false);
    }

    protected static long user(String name, boolean hidden) {
        long id = id();
        post("/v1/users", Map.of("id", id, "name", name, "presenceHidden", hidden));
        return id;
    }

    /** members: userId → rol */
    protected static long conversation(String kind, Map<Long, String> members) {
        long id = id();
        List<Map<String, Object>> ms = members.entrySet().stream()
                .map(e -> Map.<String, Object>of("userId", e.getKey(), "role", e.getValue())).toList();
        post("/v1/conversations", Map.of("id", id, "kind", kind, "title", kind.toLowerCase(), "members", ms));
        return id;
    }

    protected static long direct(long a, long b) {
        return conversation("DIRECT", Map.of(a, "MEMBER", b, "MEMBER"));
    }

    protected static <T> T bean(ConfigurableApplicationContext ctx, Class<T> type) {
        return ctx.getBean(type);
    }

    protected static JdbcClient jdbc() {
        return bean(cluster.nodeA, JdbcClient.class);
    }

    private static void post(String path, Object body) {
        try {
            HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + portA + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() >= 300) {
                throw new IllegalStateException(path + " → " + r.statusCode() + " " + r.body());
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected static String get(int port, String path, long userId) {
        try {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .header("X-User-Id", String.valueOf(userId)).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
