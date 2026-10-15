package io.paylink.gateway.admin;

import io.paylink.gateway.policy.ApiKeyHasher;
import io.paylink.gateway.policy.EndpointPattern;
import io.paylink.gateway.policy.PolicyCache;
import io.paylink.gateway.policy.PolicyChangeSubscriber;
import io.paylink.gateway.policy.PolicyRepository;
import io.paylink.gateway.policy.RateLimitPolicy;
import io.paylink.gateway.policy.ScopeType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Comparator;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Limitleri deploy olmadan değiştirmek için admin API (gereksinim 3).
 * Akış: DB'ye yaz (+audit) → Redis Pub/Sub ile haber ver → tüm pod'lar ≤ 1-2 sn içinde yeniden yükler.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final PolicyRepository repository;
    private final PolicyCache cache;
    private final PolicyChangeSubscriber changes;

    public AdminController(PolicyRepository repository, PolicyCache cache, PolicyChangeSubscriber changes) {
        this.repository = repository;
        this.cache = cache;
        this.changes = changes;
    }

    /** Bu pod'un bellekteki görünümü (hangi versiyonu biliyor?). */
    @GetMapping("/policies")
    public PoliciesView policies() {
        var snapshot = cache.current();
        List<PolicyView> items = snapshot.allPolicies().stream()
                .sorted(Comparator.comparing(RateLimitPolicy::id))
                .map(PolicyView::from)
                .toList();
        return new PoliciesView(snapshot.version(), items);
    }

    @PutMapping("/policies")
    public Mono<PolicyView> upsert(@Valid @RequestBody PolicyRequest req,
                                   @RequestHeader(value = "X-Admin-User", defaultValue = "admin") String adminUser) {
        EndpointPattern endpoint = EndpointPattern.parse(req.endpoint());
        return Mono.fromCallable(() -> repository.upsert(req.scopeType(), req.scopeId(), endpoint,
                        req.ratePerSec(), req.burst(), req.dailyQuota(), adminUser))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(saved -> changes.publishChange("policy:" + saved.id()).thenReturn(PolicyView.from(saved)));
    }

    @PostMapping("/api-keys")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<CreatedApiKey> createApiKey(@Valid @RequestBody ApiKeyRequest req) {
        String plain = ApiKeyHasher.newApiKey();
        return Mono.fromRunnable(() -> repository.insertApiKey(ApiKeyHasher.sha256Hex(plain), req.tenantId(), req.plan()))
                .subscribeOn(Schedulers.boundedElastic())
                .then(changes.publishChange("api-key:" + req.tenantId()))
                .thenReturn(new CreatedApiKey(plain, req.tenantId(), req.plan()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public java.util.Map<String, String> badRequest(IllegalArgumentException e) {
        return java.util.Map.of("error", "bad_request", "message", e.getMessage());
    }

    public record PolicyRequest(
            @NotNull ScopeType scopeType,
            @NotBlank String scopeId,
            @NotBlank String endpoint,
            @Min(1) int ratePerSec,
            @Min(1) int burst,
            @Min(0) Long dailyQuota) {

        @AssertTrue(message = "burst >= ratePerSec olmalı")
        public boolean isBurstValid() {
            return burst >= ratePerSec;
        }
    }

    public record ApiKeyRequest(
            @NotBlank String tenantId,
            @NotBlank @Pattern(regexp = "FREE|PRO|ENTERPRISE") String plan) {
    }

    /** Düz metin key sadece bu cevapta bir kez gösterilir. */
    public record CreatedApiKey(String apiKey, String tenantId, String plan) {
    }

    public record PoliciesView(long version, List<PolicyView> policies) {
    }

    public record PolicyView(long id, ScopeType scopeType, String scopeId, String endpoint,
                             int ratePerSec, int burst, Long dailyQuota, long version) {
        static PolicyView from(RateLimitPolicy p) {
            return new PolicyView(p.id(), p.scopeType(), p.scopeId(), p.endpoint().raw(),
                    p.ratePerSec(), p.burst(), p.dailyQuota(), p.version());
        }
    }
}
