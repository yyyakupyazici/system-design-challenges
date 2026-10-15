package io.paylink.gateway.filter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.paylink.gateway.limiter.RateLimitDecision;
import io.paylink.gateway.limiter.RedisRateLimiter;
import io.paylink.gateway.policy.ApiKeyHasher;
import io.paylink.gateway.policy.ApiKeyRecord;
import io.paylink.gateway.policy.PolicyCache;
import io.paylink.gateway.policy.PolicyResolver;
import io.paylink.gateway.policy.PolicyResolver.Resolution;
import io.paylink.gateway.policy.PolicySnapshot;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Her public API isteğinin geçtiği rate limit kapısı.
 *
 * <pre>
 * ① X-API-Key → rehberden tenant + plan (bellek)
 * ② Kuralları çöz: genel kova + endpoint kovası + günlük kota (bellek)
 * ③ Tenant kovaları: tek Lua çağrısı, hepsi ya da hiçbiri (Redis)
 * ④ Global kova (Redis)
 * ⑤ Geçerse downstream'e ilet; 5xx dönerse kotayı iade et
 * </pre>
 */
@Component
public class RateLimitGlobalFilter implements GlobalFilter, Ordered {

    public static final String API_KEY_HEADER = "X-API-Key";
    public static final String TENANT_HEADER = "X-Tenant-Id";

    private final PolicyCache policyCache;
    private final PolicyResolver resolver;
    private final RedisRateLimiter limiter;
    private final Clock clock;
    private final MeterRegistry meters;

    public RateLimitGlobalFilter(PolicyCache policyCache, PolicyResolver resolver, RedisRateLimiter limiter,
                                 Clock clock, MeterRegistry meters) {
        this.policyCache = policyCache;
        this.resolver = resolver;
        this.limiter = limiter;
        this.clock = clock;
        this.meters = meters;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 100; // routing filtresinden çok önce
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String apiKey = request.getHeaders().getFirst(API_KEY_HEADER);
        if (apiKey == null || apiKey.isBlank()) {
            return error(exchange, HttpStatus.UNAUTHORIZED, "missing_api_key", "X-API-Key header'ı gerekli");
        }

        PolicySnapshot snapshot = policyCache.current();
        if (!snapshot.isLoaded()) {
            return error(exchange, HttpStatus.SERVICE_UNAVAILABLE, "policies_not_loaded", "Gateway hazır değil");
        }

        ApiKeyRecord key = snapshot.apiKey(ApiKeyHasher.sha256Hex(apiKey));
        if (key == null || !key.active()) {
            return error(exchange, HttpStatus.UNAUTHORIZED, "invalid_api_key", "Geçersiz API key");
        }

        String method = request.getMethod().name();
        String path = request.getPath().pathWithinApplication().value();
        Resolution resolution = resolver.resolve(key, method, path, snapshot);
        if (!resolution.hasRules()) {
            return error(exchange, HttpStatus.FORBIDDEN, "no_policy", "Bu plan için limit kuralı tanımlı değil");
        }

        LocalDate today = LocalDate.now(clock.withZone(ZoneOffset.UTC));
        return limiter.checkTenant(resolution, today).flatMap(decision -> {
            if (!decision.allowed()) {
                count(decision, key.plan());
                return reject(exchange, decision);
            }
            return limiter.checkGlobal().flatMap(global -> {
                if (!global.allowed()) {
                    count(global, key.plan());
                    // İstek downstream'e gitmeyecek → kotadan düşmemeli.
                    return limiter.refundQuota(decision).then(reject(exchange, global));
                }
                count(decision, key.plan());
                writeRateLimitHeaders(exchange.getResponse().getHeaders(), decision);
                return forward(exchange, chain, key, decision);
            });
        });
    }

    private Mono<Void> forward(ServerWebExchange exchange, GatewayFilterChain chain,
                               ApiKeyRecord key, RateLimitDecision decision) {
        ServerHttpRequest mutated = exchange.getRequest().mutate()
                .headers(h -> {
                    h.remove(API_KEY_HEADER);           // sır downstream'e sızmasın
                    h.set(TENANT_HEADER, key.tenantId()); // client'ın gönderdiği X-Tenant-Id ezilir (spoofing'e karşı)
                })
                .build();
        ServerWebExchange forwarded = exchange.mutate().request(mutated).build();
        return chain.filter(forwarded)
                .then(Mono.defer(() -> is5xx(exchange) ? limiter.refundQuota(decision) : Mono.<Void>empty()))
                .onErrorResume(e -> limiter.refundQuota(decision).then(Mono.error(e)));
    }

    private static boolean is5xx(ServerWebExchange exchange) {
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        return status != null && status.is5xxServerError();
    }

    private Mono<Void> reject(ServerWebExchange exchange, RateLimitDecision decision) {
        HttpHeaders headers = exchange.getResponse().getHeaders();
        long retryAfterSec;
        HttpStatus status;
        String code;
        switch (decision.reason()) {
            case QUOTA -> {
                status = HttpStatus.TOO_MANY_REQUESTS;
                code = "daily_quota_exceeded";
                retryAfterSec = secondsUntilUtcMidnight();
            }
            case GLOBAL, FAIL_CLOSED -> {
                // Müşterinin suçu değil: sistem kapasitesi / store sorunu → 503
                status = HttpStatus.SERVICE_UNAVAILABLE;
                code = decision.reason() == RateLimitDecision.Reason.GLOBAL ? "system_overloaded" : "rate_limiter_unavailable";
                retryAfterSec = Math.max(1, ceilDiv(decision.retryAfterMs(), 1000));
            }
            default -> {
                status = HttpStatus.TOO_MANY_REQUESTS;
                code = "rate_limited";
                retryAfterSec = Math.max(1, ceilDiv(decision.retryAfterMs(), 1000));
            }
        }
        if (decision.reason() != RateLimitDecision.Reason.GLOBAL) {
            writeRateLimitHeaders(headers, decision);
        }
        headers.set(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSec));
        return error(exchange, status, code, "Retry-After: " + retryAfterSec + " sn");
    }

    private static void writeRateLimitHeaders(HttpHeaders headers, RateLimitDecision d) {
        if (d.limit() >= 0) {
            headers.set("X-RateLimit-Limit", String.valueOf(d.limit()));
            headers.set("X-RateLimit-Remaining", String.valueOf(Math.max(0, d.remaining())));
            headers.set("X-RateLimit-Reset", String.valueOf(ceilDiv(d.resetMs(), 1000)));
        }
        if (d.quotaRemaining() >= 0) {
            headers.set("X-Quota-Remaining", String.valueOf(d.quotaRemaining()));
        }
    }

    private long secondsUntilUtcMidnight() {
        ZonedDateTime now = ZonedDateTime.now(clock.withZone(ZoneOffset.UTC));
        ZonedDateTime midnight = now.toLocalDate().plusDays(1).atStartOfDay(ZoneOffset.UTC);
        return Math.max(1, Duration.between(now, midnight).toSeconds());
    }

    private static long ceilDiv(long a, long b) {
        return Math.ceilDiv(Math.max(0, a), b);
    }

    private void count(RateLimitDecision decision, String plan) {
        // Tenant id'yi tag yapmıyoruz: 8.000 tenant = yüksek kardinalite. Tenant bazlı analiz log/trace'ten.
        Counter.builder("paylink.ratelimit.decisions")
                .tag("result", decision.reason().name().toLowerCase())
                .tag("plan", plan)
                .register(meters)
                .increment();
    }

    private static Mono<Void> error(ServerWebExchange exchange, HttpStatus status, String code, String message) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"error\":\"" + code + "\",\"message\":\"" + message.replace("\"", "'") + "\"}";
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8))));
    }
}
