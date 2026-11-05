package io.paramix.llmgw.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.paramix.llmgw.provider.FakeChatModel;
import io.paramix.llmgw.provider.FakeChatModel.Outcome;
import io.paramix.llmgw.provider.ProviderRegistry;
import io.paramix.llmgw.usage.UsageLedger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Uçtan uca: gerçek Redis + PostgreSQL, sağlayıcılar FAKE (gerçek API'ye hiç gidilmez).
 * Fake model cevabı "[sağlayıcı-model] son mesaj" — maskeleme ve geri açma bu sayede görülebilir.
 */
@EnabledIf("io.paramix.llmgw.it.TestInfra#available")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class GatewayFlowIT {

    static final String SUPPORT = "sk-support-demo";
    static final String BATCH = "sk-batch-demo";

    @DynamicPropertySource
    static void infra(DynamicPropertyRegistry r) {
        r.add("spring.data.redis.host", TestInfra::redisHost);
        r.add("spring.data.redis.port", TestInfra::redisPort);
        r.add("spring.datasource.url", TestInfra::pgUrl);
        r.add("spring.datasource.username", TestInfra::pgUser);
        r.add("spring.datasource.password", TestInfra::pgPassword);
    }

    @LocalServerPort int port;
    @Autowired ProviderRegistry providers;
    @Autowired StringRedisTemplate redis;
    @Autowired UsageLedger ledger;
    final ObjectMapper json = new ObjectMapper();
    final HttpClient http = HttpClient.newHttpClient();

    FakeChatModel primary;
    FakeChatModel backup;
    FakeChatModel gpu;

    @BeforeEach
    void reset() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        primary = providers.fake("primary");
        backup = providers.fake("backup");
        gpu = providers.fake("gpu");
        for (String p : List.of("primary", "backup", "gpu")) {
            providers.fake(p).reset();
            providers.breaker(p).reset();
        }
    }

    // ------------------------------------------------------------------ yardımcılar

    record Res(int status, JsonNode body, HttpResponse<String> raw) {
    }

    Res post(String key, Map<String, String> headers, Object body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/chat/completions"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + key)
                .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
        headers.forEach(b::header);
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        JsonNode node = r.body().startsWith("{") ? json.readTree(r.body()) : null;
        return new Res(r.statusCode(), node, r);
    }

    Res ask(String key, String model, String question) throws Exception {
        return post(key, Map.of("X-User-Id", "u1"), Map.of("model", model,
                "messages", List.of(Map.of("role", "user", "content", question))));
    }

    static String content(Res r) {
        return r.body().at("/choices/0/message/content").asText();
    }

    static String gw(Res r, String field) {
        return r.body().at("/x_gateway/" + field).asText();
    }

    /** SSE gövdesini çözer: data satırları; içerik parçaları birleştirilir. */
    record Stream(String text, List<JsonNode> chunks, boolean done) {
        JsonNode last() {
            return chunks.getLast();
        }
    }

    Stream stream(String key, Map<String, String> headers, String question) throws Exception {
        Res r = post(key, headers, Map.of("model", "auto", "stream", true,
                "messages", List.of(Map.of("role", "user", "content", question))));
        assertThat(r.status()).isEqualTo(200);
        StringBuilder text = new StringBuilder();
        List<JsonNode> chunks = new ArrayList<>();
        boolean done = false;
        for (String line : r.raw().body().split("\n")) {
            if (!line.startsWith("data:")) {
                continue;
            }
            String data = line.substring(5).trim();
            if (data.equals("[DONE]")) {
                done = true;
                continue;
            }
            JsonNode n = json.readTree(data);
            chunks.add(n);
            text.append(n.at("/choices/0/delta/content").asText(""));
        }
        return new Stream(text.toString(), chunks, done);
    }

    // ------------------------------------------------------------------ senaryolar

    @Test
    void simpleQuestionGoesToSmallModelAndSecondTimeComesFromExactCache() throws Exception {
        Res first = ask(SUPPORT, "auto", "EFT ile havale farkı nedir?");
        assertThat(first.status()).isEqualTo(200);
        assertThat(content(first)).isEqualTo("[small-model] EFT ile havale farkı nedir?");
        assertThat(gw(first, "routedModel")).isEqualTo("small");
        assertThat(gw(first, "cache")).isEqualTo("MISS");
        assertThat(first.body().at("/x_gateway/costCredits").asDouble()).isPositive();

        Res second = ask(SUPPORT, "auto", "EFT ile havale farkı nedir?");
        assertThat(gw(second, "cache")).isEqualTo("HIT_EXACT");
        assertThat(second.body().at("/x_gateway/costCredits").asDouble()).isZero();
        assertThat(primary.callCount()).isEqualTo(1);

        String id = first.body().get("id").asText();
        await().atMost(Duration.ofSeconds(5)).until(() -> ledger.find(id) != null);
        assertThat(ledger.find(id)).containsEntry("routed_model", "small").containsEntry("cache_status", "MISS");
    }

    @Test
    void similarQuestionHitsSemanticCacheButDifferentNumbersDoNot() throws Exception {
        ask(SUPPORT, "auto", "Kredi kartı yıllık aidatı nasıl iade edilir?");
        Res similar = ask(SUPPORT, "auto", "kredi kartı yıllık aidatı nasıl iade edilir");
        assertThat(gw(similar, "cache")).isEqualTo("HIT_SEMANTIC");

        ask(SUPPORT, "auto", "Günlük 500 TL ATM limiti nasıl artırılır?");
        Res otherNumber = ask(SUPPORT, "auto", "Günlük 600 TL ATM limiti nasıl artırılır?");
        assertThat(gw(otherNumber, "cache")).isEqualTo("MISS");
        assertThat(primary.callCount()).isEqualTo(3);
    }

    @Test
    void piiIsMaskedBeforeTheProviderAndRestoredInTheAnswerAndNeverCached() throws Exception {
        String q = "Bilgilerimi ali@example.com adresine yollar mısınız? TC 10000000146";
        Res r = ask(SUPPORT, "auto", q);
        assertThat(content(r)).isEqualTo("[small-model] " + q);
        assertThat(r.body().at("/x_gateway/piiMasked").asInt()).isEqualTo(2);
        List<String> sent = primary.calls().getFirst().messages();
        assertThat(String.join(" ", sent)).contains("<EMAIL_1>", "<TCKN_1>").doesNotContain("ali@example.com", "10000000146");

        assertThat(gw(ask(SUPPORT, "auto", q), "cache")).isEqualTo("MISS");
        assertThat(primary.callCount()).isEqualTo(2);

        String id = r.body().get("id").asText();
        await().atMost(Duration.ofSeconds(5)).until(() -> ledger.find(id) != null);
        assertThat(String.valueOf(ledger.find(id).get("masked_prompt"))).doesNotContain("ali@example.com");
    }

    @Test
    void personalDataQuestionIsNotCached() throws Exception {
        ask(SUPPORT, "auto", "Hesabımda ne kadar para var?");
        assertThat(gw(ask(SUPPORT, "auto", "Hesabımda ne kadar para var?"), "cache")).isEqualTo("MISS");
    }

    @Test
    void streamIsUnmaskedOnTheFlyAndEndsWithUsageAndDone() throws Exception {
        Stream s = stream(SUPPORT, Map.of("X-User-Id", "u2"), "Adresim ali@example.com doğru mu?");
        assertThat(s.text()).isEqualTo("[small-model] Adresim ali@example.com doğru mu?");
        assertThat(s.done()).isTrue();
        assertThat(s.last().at("/choices/0/finish_reason").asText()).isEqualTo("stop");
        assertThat(s.last().at("/usage/completion_tokens").asInt()).isPositive();
        assertThat(s.last().at("/x_gateway/provider").asText()).isEqualTo("primary");
    }

    @Test
    void streamFallsBackWhenPrimaryFailsBeforeTheFirstToken() throws Exception {
        primary.script(Outcome.FAIL_BEFORE_FIRST_TOKEN);
        Stream s = stream(SUPPORT, Map.of(), "Şube saatleri nedir?");
        assertThat(s.text()).isEqualTo("[small-backup-model] Şube saatleri nedir?");
        assertThat(s.last().at("/x_gateway/fallbackFrom").asText()).isEqualTo("small");
        assertThat(s.last().at("/x_gateway/routedModel").asText()).isEqualTo("small-backup");
    }

    @Test
    void streamFallsBackWhenTheFirstTokenIsTooSlow() throws Exception {
        primary.slowDelay(Duration.ofSeconds(3));
        primary.script(Outcome.SLOW_FIRST_TOKEN);
        Stream s = stream(SUPPORT, Map.of(), "Kartım kayboldu ne yapmalıyım?");
        assertThat(s.text()).startsWith("[small-backup-model]");
    }

    @Test
    void midStreamFailureIsReportedNotStitchedWithAnotherModel() throws Exception {
        primary.script(Outcome.FAIL_MID_STREAM);
        Stream s = stream(SUPPORT, Map.of(), "Bana uzunca kredi kartı başvuru sürecini anlat");
        assertThat(s.chunks().getLast().at("/error/code").asText()).isEqualTo("stream_interrupted");
        assertThat(s.done()).isTrue();
        assertThat(backup.callCount()).isZero();
    }

    @Test
    void nonStreamFallsBackToBackupProvider() throws Exception {
        primary.script(Outcome.FAIL_BEFORE_FIRST_TOKEN);
        Res r = ask(SUPPORT, "auto", "IBAN nedir?");
        assertThat(r.status()).isEqualTo(200);
        assertThat(gw(r, "provider")).isEqualTo("backup");
        assertThat(gw(r, "fallbackFrom")).isEqualTo("small");
    }

    @Test
    void confidentialDataOnlyGoesToTheInternalGpu() throws Exception {
        Res auto = post(SUPPORT, Map.of("X-Data-Class", "confidential"), Map.of("model", "auto",
                "messages", List.of(Map.of("role", "user", "content", "Bu müşterinin risk notunu analiz et"))));
        assertThat(gw(auto, "provider")).isEqualTo("gpu");
        assertThat(primary.callCount()).isZero();

        Res external = post(SUPPORT, Map.of("X-Data-Class", "gizli"), Map.of("model", "large",
                "messages", List.of(Map.of("role", "user", "content", "x"))));
        assertThat(external.status()).isEqualTo(403);
        assertThat(external.body().at("/error/code").asText()).isEqualTo("data_class_violation");
    }

    @Test
    void overBudgetIsRejectedWith402() throws Exception {
        Res r = post("sk-tiny", Map.of(), Map.of("model", "large", "max_tokens", 4000,
                "messages", List.of(Map.of("role", "user", "content", "x"))));
        assertThat(r.status()).isEqualTo(402);
        assertThat(r.body().at("/error/code").asText()).isEqualTo("budget_exceeded");
        assertThat(primary.callCount()).isZero();
    }

    @Test
    void overBudgetDowngradesToTheCheapestModelWhenTheTeamPrefersIt() throws Exception {
        Res r = post("sk-tinydown", Map.of(), Map.of("model", "large", "max_tokens", 4000,
                "messages", List.of(Map.of("role", "user", "content", "Uzun bir analiz yaz"))));
        assertThat(r.status()).isEqualTo(200);
        assertThat(gw(r, "routedModel")).isEqualTo("internal");
    }

    @Test
    void teamTokenPerMinuteLimitReturns429WithRetryAfter() throws Exception {
        Map<String, Object> body = Map.of("model", "small", "max_tokens", 1000,
                "messages", List.of(Map.of("role", "user", "content", "Kısa bir soru")));
        assertThat(post("sk-slow", Map.of(), body).status()).isEqualTo(200);
        // ilk istek gerçekte az token harcadı ve iade edildi; ikisi aynı anda açık olsaydı 2×1000 > 1500
        Res second = post("sk-slow", Map.of(), Map.of("model", "small", "max_tokens", 1490,
                "messages", List.of(Map.of("role", "user", "content", "Kısa bir soru 2"))));
        assertThat(second.status()).isEqualTo(429);
        assertThat(second.raw().headers().firstValue("Retry-After")).isPresent();
    }

    @Test
    void batchJobsDoNotPayThePromptCacheWritePremiumButChatsDo() throws Exception {
        String system = "Sen bir bankacılık asistanısın. ".repeat(200);
        Map<String, Object> body = Map.of("model", "small", "messages", List.of(
                Map.of("role", "system", "content", system), Map.of("role", "user", "content", "Özetle")));
        Res batch = post(BATCH, Map.of(), body);
        assertThat(batch.body().at("/x_gateway/promptCache").asBoolean()).isFalse();

        Res chat = post(SUPPORT, Map.of("X-Session-Id", "s-42"), body);
        assertThat(chat.body().at("/x_gateway/promptCache").asBoolean()).isTrue();
        assertThat(chat.body().at("/usage/prompt_tokens_details/cache_write_tokens").asInt()).isPositive();
    }

    @Test
    void invalidKeyIs401AndUsageEndpointShowsTheBudget() throws Exception {
        assertThat(ask("sk-wrong", "auto", "x").status()).isEqualTo(401);
        ask(SUPPORT, "large", "Bir strateji öner");
        HttpResponse<String> u = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/usage"))
                .header("Authorization", "Bearer " + SUPPORT).header("X-User-Id", "u1").GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode n = json.readTree(u.body());
        assertThat(n.at("/budget/team").asText()).isEqualTo("support");
        assertThat(n.at("/budget/spentCredits").asDouble()).isPositive();
    }
}
