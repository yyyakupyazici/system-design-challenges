package io.paylink.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Uçtan uca: gerçek Redis + PostgreSQL (Flyway seed verisi) + sahte payment-service.
 * Seed: demo-free-key (FREE, 10/s burst 10, POST /payments 5/s, kota 50K), demo-pro-key (PRO), demo-ent-key (tenant_42).
 */
@EnabledIf("io.paylink.gateway.it.TestInfra#available")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayRateLimitIT {

    private static final String ADMIN_TOKEN = "test-admin-token";
    private static final HttpServer DOWNSTREAM = startDownstream();
    private static final AtomicReference<String> LAST_TENANT_HEADER = new AtomicReference<>();
    private static final AtomicReference<String> LAST_API_KEY_HEADER = new AtomicReference<>();
    private static final AtomicInteger DOWNSTREAM_CALLS = new AtomicInteger();

    @Autowired
    WebTestClient web;

    @Autowired
    ReactiveStringRedisTemplate redis;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.data.redis.host", TestInfra::redisHost);
        r.add("spring.data.redis.port", TestInfra::redisPort);
        r.add("spring.datasource.url", TestInfra::pgUrl);
        r.add("spring.datasource.username", TestInfra::pgUser);
        r.add("spring.datasource.password", TestInfra::pgPassword);
        r.add("paylink.payment-service-url", () -> "http://localhost:" + DOWNSTREAM.getAddress().getPort());
        r.add("paylink.admin-token", () -> ADMIN_TOKEN);
        r.add("ratelimit.global.rate-per-sec", () -> 100000);
        r.add("ratelimit.global.burst", () -> 100000);
    }

    @AfterAll
    static void stopDownstream() {
        DOWNSTREAM.stop(0);
    }

    @BeforeEach
    void cleanRedis() {
        redis.execute(c -> c.serverCommands().flushDb()).blockLast();
    }

    @Test
    void missingOrUnknownApiKey_is401() {
        web.get().uri("/payments/pl_1").exchange().expectStatus().isUnauthorized();
        web.get().uri("/payments/pl_1").header("X-API-Key", "nope").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void freePlan_postPayments_endpointBucketHitsFirst_then429WithHeaders() {
        List<EntityExchangeResult<byte[]>> results = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            results.add(web.post().uri("/payments").header("X-API-Key", "demo-free-key")
                    .contentType(MediaType.APPLICATION_JSON).bodyValue("{\"amount\":450}")
                    .exchange().expectBody().returnResult());
        }

        // Endpoint kovası (POST /payments: 5/s, burst 5) genel kovadan (10/s) önce biter.
        assertThat(results.subList(0, 5)).allMatch(r -> r.getStatus().value() == 201);
        EntityExchangeResult<byte[]> rejected = results.stream()
                .filter(r -> r.getStatus().value() == 429).findFirst().orElseThrow();
        assertThat(rejected.getResponseHeaders().getFirst("Retry-After")).isNotNull();
        assertThat(rejected.getResponseHeaders().getFirst("X-RateLimit-Limit")).isEqualTo("5");
        assertThat(rejected.getResponseHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("0");
        assertThat(new String(rejected.getResponseBody(), StandardCharsets.UTF_8)).contains("rate_limited");
    }

    @Test
    void tenantsAreIsolated_noisyTenantDoesNotAffectOthers() {
        // FREE tenant kovasını boşaltır…
        List<Integer> noisy = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            noisy.add(web.get().uri("/payments/pl_1").header("X-API-Key", "demo-free-key")
                    .exchange().expectBody().returnResult().getStatus().value());
        }
        assertThat(noisy).contains(429);
        // …PRO tenant etkilenmez.
        web.get().uri("/payments/pl_1").header("X-API-Key", "demo-pro-key").exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-RateLimit-Limit", "200");
    }

    @Test
    void forwardsTenantHeader_stripsApiKey_ignoresSpoofedTenant() {
        web.get().uri("/payments/pl_1").header("X-API-Key", "demo-ent-key").header("X-Tenant-Id", "evil")
                .exchange().expectStatus().isOk();

        assertThat(LAST_TENANT_HEADER.get()).isEqualTo("tenant_42");
        assertThat(LAST_API_KEY_HEADER.get()).isNull();
    }

    @Test
    void dailyQuota_isRefundedOn5xx() {
        int before = DOWNSTREAM_CALLS.get();
        web.get().uri("/payments/boom").header("X-API-Key", "demo-free-key").exchange()
                .expectStatus().is5xxServerError();
        assertThat(DOWNSTREAM_CALLS.get()).isEqualTo(before + 1);

        String quota = redis.opsForValue().get("quota:{tenant_free_1}:" + java.time.LocalDate.now(java.time.ZoneOffset.UTC)).block();
        assertThat(quota).isEqualTo("0");
    }

    @Test
    void adminChange_propagatesWithoutDeploy() {
        // Yeni tenant için key oluştur (düz metin sadece bu cevapta döner)
        Map<?, ?> created = web.post().uri("/admin/api-keys").header("X-Admin-Token", ADMIN_TOKEN)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("tenantId", "tenant_admin_it", "plan", "PRO"))
                .exchange().expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        String apiKey = (String) created.get("apiKey");
        assertThat(apiKey).startsWith("pl_live_");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                web.get().uri("/payments/pl_1").header("X-API-Key", apiKey).exchange()
                        .expectHeader().valueEquals("X-RateLimit-Limit", "200"));

        // Deploy olmadan limit değişikliği: DB → Pub/Sub → tüm pod'lar reload
        web.put().uri("/admin/policies").header("X-Admin-Token", ADMIN_TOKEN)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("scopeType", "TENANT", "scopeId", "tenant_admin_it", "endpoint", "*",
                        "ratePerSec", 3, "burst", 3))
                .exchange().expectStatus().isOk();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                web.get().uri("/payments/pl_1").header("X-API-Key", apiKey).exchange()
                        .expectHeader().valueEquals("X-RateLimit-Limit", "3"));
    }

    @Test
    void adminApi_rejectsInvalidPolicy() {
        web.put().uri("/admin/policies").header("X-Admin-Token", ADMIN_TOKEN)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("scopeType", "PLAN", "scopeId", "PRO", "endpoint", "payments",
                        "ratePerSec", 10, "burst", 10))
                .exchange().expectStatus().isBadRequest();
        web.put().uri("/admin/policies").header("X-Admin-Token", ADMIN_TOKEN)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("scopeType", "PLAN", "scopeId", "PRO", "endpoint", "*",
                        "ratePerSec", 10, "burst", 5))
                .exchange().expectStatus().isBadRequest();
    }

    @Test
    void adminApi_requiresToken() {
        web.get().uri("/admin/policies").exchange().expectStatus().isUnauthorized();
        web.get().uri("/admin/policies").header("X-Admin-Token", ADMIN_TOKEN).exchange().expectStatus().isOk();
    }

    /** Sahte payment-service: /payments/boom → 500, POST → 201, diğerleri → 200. */
    private static HttpServer startDownstream() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/", exchange -> {
                DOWNSTREAM_CALLS.incrementAndGet();
                LAST_TENANT_HEADER.set(exchange.getRequestHeaders().getFirst("X-Tenant-Id"));
                LAST_API_KEY_HEADER.set(exchange.getRequestHeaders().getFirst("X-API-Key"));
                exchange.getRequestBody().readAllBytes();
                int status = exchange.getRequestURI().getPath().endsWith("/boom") ? 500
                        : "POST".equals(exchange.getRequestMethod()) ? 201 : 200;
                byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
