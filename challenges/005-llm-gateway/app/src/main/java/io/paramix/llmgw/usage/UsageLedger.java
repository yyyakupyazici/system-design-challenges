package io.paramix.llmgw.usage;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Kullanım defteri: maliyet raporları ve bütçe mutabakatının gerçeğin kaynağı. Kullanıcıyı bekletmemek için
 * asenkron yazılır (sınırlı kuyruk; dolarsa yazan thread kendisi yazar = backpressure, kayıt düşmez).
 * Ölçek büyüyünce: olay Kafka'ya, oradan ClickHouse'a (README).
 */
@Component
public class UsageLedger {

    private static final Logger log = LoggerFactory.getLogger(UsageLedger.class);
    private static final int MAX_TEXT = 4000;

    private final JdbcClient jdbc;
    private final ThreadPoolExecutor writer = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10_000), new ThreadPoolExecutor.CallerRunsPolicy());

    public UsageLedger(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void record(UsageEvent e) {
        writer.execute(() -> {
            try {
                insert(e);
            } catch (RuntimeException ex) {
                log.error("Kullanım kaydı yazılamadı {}: {}", e.requestId(), ex.getMessage());
            }
        });
    }

    void insert(UsageEvent e) {
        jdbc.sql("""
                        INSERT INTO usage_event (request_id, at, team, app, user_id, requested_model, routed_model, provider,
                            fallback_from, cache_status, prompt_cache, input_tokens, cache_read_tokens, cache_write_tokens,
                            output_tokens, cost_micro, latency_ms, ttft_ms, status, pii_masked, masked_prompt, masked_response)
                        VALUES (:id, :at, :team, :app, :user, :req, :routed, :provider, :fb, :cache, :pc, :in, :cr, :cw,
                            :out, :cost, :lat, :ttft, :status, :pii, :prompt, :resp)
                        ON CONFLICT (request_id) DO NOTHING""")
                .param("id", e.requestId()).param("at", Timestamp.from(e.at())).param("team", e.team())
                .param("app", e.app()).param("user", e.userId()).param("req", e.requestedModel())
                .param("routed", e.routedModel()).param("provider", e.provider()).param("fb", e.fallbackFrom())
                .param("cache", e.cacheStatus()).param("pc", e.promptCache()).param("in", e.inputTokens())
                .param("cr", e.cacheReadTokens()).param("cw", e.cacheWriteTokens()).param("out", e.outputTokens())
                .param("cost", e.costMicro()).param("lat", e.latencyMs()).param("ttft", e.ttftMs())
                .param("status", e.status()).param("pii", e.piiMasked())
                .param("prompt", truncate(e.maskedPrompt())).param("resp", truncate(e.maskedResponse()))
                .update();
    }

    /** Bugünün ekip raporu: model ve cache durumuna göre istek, token, kredi. */
    public List<Map<String, Object>> teamSummary(String team) {
        return jdbc.sql("""
                        SELECT routed_model, cache_status, count(*) AS requests,
                               sum(input_tokens) AS input_tokens, sum(cache_read_tokens) AS cache_read_tokens,
                               sum(cache_write_tokens) AS cache_write_tokens, sum(output_tokens) AS output_tokens,
                               round(sum(cost_micro) / 1000000.0, 6) AS cost_credits
                        FROM usage_event WHERE team = :team AND at >= date_trunc('day', now())
                        GROUP BY routed_model, cache_status ORDER BY cost_credits DESC NULLS LAST""")
                .param("team", team).query().listOfRows();
    }

    public Map<String, Object> find(String requestId) {
        return jdbc.sql("SELECT * FROM usage_event WHERE request_id = :id").param("id", requestId)
                .query().listOfRows().stream().findFirst().orElse(null);
    }

    /** Maskelenmiş içerik 30 gün saklanır (prod'da günlük partition + DROP PARTITION daha ucuz). */
    @Scheduled(cron = "${gateway.usage.retention-cron:0 30 3 * * *}")
    public void purgeContent() {
        int n = jdbc.sql("""
                        UPDATE usage_event SET masked_prompt = NULL, masked_response = NULL
                        WHERE at < now() - interval '30 days' AND masked_prompt IS NOT NULL""").update();
        if (n > 0) {
            log.info("30 günü geçen {} kaydın içeriği silindi", n);
        }
    }

    @PreDestroy
    void shutdown() throws InterruptedException {
        writer.shutdown();
        writer.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static String truncate(String s) {
        return s == null || s.length() <= MAX_TEXT ? s : s.substring(0, MAX_TEXT);
    }
}
