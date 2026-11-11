package io.paramix.provision;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Testlerin servisle konuştuğu gerçek HTTP istemcisi (thread-safe; paralel testlerde paylaşılır). */
public final class TestApi {

    public record Response(int status, HttpHeaders headers, String body) {

        private static final ObjectMapper JSON = new ObjectMapper();

        public JsonNode json() {
            try {
                return JSON.readTree(body);
            } catch (IOException e) {
                throw new IllegalStateException("JSON değil: " + body, e);
            }
        }

        public String field(String name) {
            JsonNode n = json().get(name);
            return n == null || n.isNull() ? null : n.asText();
        }

        public String code() {
            return field("code");
        }

        public boolean replayed() {
            return headers.firstValue("Idempotent-Replayed").map(Boolean::parseBoolean).orElse(false);
        }
    }

    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final String base;

    public TestApi(int port) {
        this.base = "http://localhost:" + port;
    }

    public Response createProvision(long customerId, String key, String asset, String amount, String ref) {
        return post("/v1/provisions", customerId, key, """
                {"asset":"%s","amount":%s,"operationType":"TRADE","operationRef":"%s"}""".formatted(asset, amount, ref));
    }

    public Response capture(long customerId, String key, String provisionId, String amount) {
        return post("/v1/provisions/" + provisionId + "/capture", customerId, key,
                amount == null ? "{}" : "{\"amount\":" + amount + "}");
    }

    public Response release(long customerId, String key, String provisionId) {
        return post("/v1/provisions/" + provisionId + "/release", customerId, key, "{}");
    }

    public Response withdraw(long customerId, String key, String asset, String amount, String destination) {
        return post("/v1/withdrawals", customerId, key, """
                {"asset":"%s","amount":"%s","destination":"%s"}""".formatted(asset, amount, destination));
    }

    public Response deposit(long customerId, String asset, String amount, String source, String ref) {
        return post("/internal/deposits", null, null, """
                {"customerId":%d,"asset":"%s","amount":"%s","source":"%s","externalRef":"%s"}"""
                .formatted(customerId, asset, amount, source, ref));
    }

    public Response post(String path, Long customerId, String key, String body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (customerId != null) {
            b.header("X-Customer-Id", customerId.toString());
        }
        if (key != null) {
            b.header("Idempotency-Key", key);
        }
        return send(b.build());
    }

    public Response get(String path, long customerId) {
        return send(HttpRequest.newBuilder(URI.create(base + path)).header("X-Customer-Id", Long.toString(customerId))
                .GET().build());
    }

    private Response send(HttpRequest request) {
        try {
            HttpResponse<String> r = http.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(r.statusCode(), r.headers(), r.body());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
