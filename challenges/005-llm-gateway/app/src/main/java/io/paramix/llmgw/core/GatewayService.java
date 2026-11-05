package io.paramix.llmgw.core;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.paramix.llmgw.api.ChatCompletionRequest;
import io.paramix.llmgw.api.ChatCompletionRequest.Message;
import io.paramix.llmgw.api.ChatCompletionResponse;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.budget.BudgetService;
import io.paramix.llmgw.budget.Reservation;
import io.paramix.llmgw.cache.CacheDecision;
import io.paramix.llmgw.cache.CachePolicy;
import io.paramix.llmgw.cache.CachedAnswer;
import io.paramix.llmgw.cache.ExactCache;
import io.paramix.llmgw.cache.PromptCachePolicy;
import io.paramix.llmgw.cache.SemanticCache;
import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.AppType;
import io.paramix.llmgw.config.GatewayProperties.ModelProperties;
import io.paramix.llmgw.config.GatewayProperties.OverBudget;
import io.paramix.llmgw.config.GatewayProperties.ProviderProperties;
import io.paramix.llmgw.config.GatewayProperties.TeamProperties;
import io.paramix.llmgw.limit.TokenRateLimiter;
import io.paramix.llmgw.pii.PiiMasker;
import io.paramix.llmgw.pii.PiiVault;
import io.paramix.llmgw.pii.StreamingUnmasker;
import io.paramix.llmgw.pricing.CostCalculator;
import io.paramix.llmgw.pricing.TokenEstimator;
import io.paramix.llmgw.pricing.TokenUsage;
import io.paramix.llmgw.provider.ProviderReply;
import io.paramix.llmgw.provider.ProviderRegistry;
import io.paramix.llmgw.provider.ProviderRequest;
import io.paramix.llmgw.routing.Router;
import io.paramix.llmgw.usage.UsageEvent;
import io.paramix.llmgw.usage.UsageLedger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Bir isteğin gateway içindeki yolculuğu. Sıra bilinçli: ucuz ve reddedebilen adımlar önde, veriyi değiştiren
 * adım (PII maskeleme) veriyi dışarı çıkaran her adımdan (cache, sağlayıcı, log) önce.
 *
 * <pre>
 *  ① doğrula + girdi tahmini         ② PII maskele             ③ yönlendir (auto, gizli veri, fallback zinciri)
 *  ④ cache bak (exact → semantic)    isabet → ⑤–⑦ atlanır, maliyet 0
 *  ⑤ ekip token/dk limiti            ⑥ bütçe rezervasyonu (en fazla tutar; yetmezse ucuz modele düş ya da 402)
 *  ⑦ sağlayıcı (circuit breaker, sağlayıcı kapasitesi; stream'de fallback sadece ilk token'dan önce)
 *  ⑧ PII geri aç                     ⑨ mutabakat (gerçek maliyet, iade), cache yaz, kullanım defteri
 * </pre>
 */
@Service
public class GatewayService {

    private static final Logger log = LoggerFactory.getLogger(GatewayService.class);

    private final GatewayProperties props;
    private final ProviderRegistry providers;
    private final TokenRateLimiter limiter;
    private final BudgetService budget;
    private final ExactCache exactCache;
    private final SemanticCache semanticCache;
    private final UsageLedger ledger;
    private final ObjectMapper json;
    private final Clock clock;
    private final MeterRegistry meters;

    public GatewayService(GatewayProperties props, ProviderRegistry providers, TokenRateLimiter limiter,
                          BudgetService budget, ExactCache exactCache, SemanticCache semanticCache, UsageLedger ledger,
                          ObjectMapper json, Clock clock, MeterRegistry meters) {
        this.props = props;
        this.providers = providers;
        this.limiter = limiter;
        this.budget = budget;
        this.exactCache = exactCache;
        this.semanticCache = semanticCache;
        this.ledger = ledger;
        this.json = json;
        this.clock = clock;
        this.meters = meters;
    }

    // ------------------------------------------------------------------ hazırlık

    record Prepared(ClientContext ctx, ChatCompletionRequest req, String requestId, long startNanos, long created,
                    List<Message> masked, PiiVault vault, String maskedLastUser, int inputTokens,
                    CacheDecision cache, String exactKey, String semanticScope, boolean promptCache) {
    }

    record Admission(Router.Plan plan, Reservation reservation, long teamTokens) {
    }

    record Hit(String status, CachedAnswer answer) {
    }

    /** Bir sağlayıcı denemesi. */
    record Attempt(String alias, ModelProperties model, String provider, long providerTokens, String fallbackFrom) {
    }

    private Prepared prepare(ClientContext ctx, ChatCompletionRequest req) {
        int inputTokens = TokenEstimator.estimateMessages(req.messages().stream().map(Message::content).toList());
        if (inputTokens > props.limits().maxInputTokens()) {
            throw new GatewayException(HttpStatus.PAYLOAD_TOO_LARGE, "context_too_large",
                    "Girdi ~" + inputTokens + " token; bu uygulama için üst sınır " + props.limits().maxInputTokens()
                            + ". Eski turları özetleyin ya da kısaltın.");
        }
        PiiVault vault = new PiiVault();
        List<Message> masked = req.messages().stream()
                .map(m -> new Message(m.role(), PiiMasker.mask(m.content(), vault))).toList();
        String lastUser = masked.stream().filter(m -> "user".equals(m.role())).reduce((a, b) -> b)
                .map(Message::content).orElse("");
        CacheDecision cd = CachePolicy.decide(ctx, req, lastUser, vault.size(), props.cache());
        boolean pc = PromptCachePolicy.usePromptCache(ctx, masked, props.cache().promptCacheMinPrefixTokens());
        return new Prepared(ctx, req, "req_" + UUID.randomUUID().toString().replace("-", ""), System.nanoTime(),
                clock.instant().getEpochSecond(), masked, vault, lastUser, inputTokens, cd, null, null, pc);
    }

    /** Yönlendirme planı + cache anahtarları (cache model sürümüyle kapsamlanır). */
    private Prepared withCacheKeys(Prepared p, Router.Plan plan) {
        ModelProperties primary = props.models().get(plan.primary());
        String exactKey = p.cache().exact()
                ? ExactCache.key(p.ctx().team(), p.ctx().app(), plan.primary(), primary.version(), p.req().maxTokens(),
                p.req().temperature(), p.masked()) : null;
        String scope = p.cache().semantic()
                ? SemanticCache.scope(p.ctx().team(), p.ctx().app(), plan.primary(), primary.version()) : null;
        return new Prepared(p.ctx(), p.req(), p.requestId(), p.startNanos(), p.created(), p.masked(), p.vault(),
                p.maskedLastUser(), p.inputTokens(), p.cache(), exactKey, scope, p.promptCache());
    }

    private Optional<Hit> lookupCache(Prepared p) {
        if (p.exactKey() != null) {
            Optional<CachedAnswer> a = exactCache.get(p.exactKey());
            if (a.isPresent()) {
                return Optional.of(new Hit("HIT_EXACT", a.get()));
            }
        }
        if (p.semanticScope() != null) {
            Optional<CachedAnswer> a = semanticCache.lookup(p.semanticScope(), p.maskedLastUser());
            if (a.isPresent()) {
                return Optional.of(new Hit("HIT_SEMANTIC", a.get()));
            }
        }
        return Optional.empty();
    }

    private Admission admit(Prepared p, Router.Plan plan) {
        ClientContext ctx = p.ctx();
        TeamProperties team = props.teams().get(ctx.team());
        ModelProperties primary = props.models().get(plan.primary());
        long teamTokens = p.inputTokens() + (long) maxTokens(primary, p.req());
        TokenRateLimiter.Decision d = limiter.tryConsume("team:" + ctx.team(), team.tokensPerMinute(), teamTokens);
        if (!d.allowed()) {
            throw new GatewayException(HttpStatus.TOO_MANY_REQUESTS, "rate_limited",
                    "Ekibin dakikalık token limiti doldu", Math.max(1, (d.waitMs() + 999) / 1000));
        }
        try {
            BudgetService.Outcome o = reserve(p, plan);
            if (!o.granted() && team.overBudget() == OverBudget.DOWNGRADE) {
                Optional<String> cheap = Router.cheapest(props, ctx.confidential());
                if (cheap.isPresent() && !cheap.get().equals(plan.primary())) {
                    plan = new Router.Plan(cheap.get(), List.of(cheap.get()));
                    o = reserve(p, plan);
                    meters.counter("llmgw.budget.downgraded", "team", ctx.team()).increment();
                }
            }
            if (!o.granted()) {
                limiter.refund("team:" + ctx.team(), team.tokensPerMinute(), teamTokens);
                throw new GatewayException(HttpStatus.PAYMENT_REQUIRED, "budget_exceeded",
                        o.denial() == BudgetService.Denial.USER_CEILING
                                ? "Bugünkü kişisel kredi tavanına ulaştınız"
                                : "Ekibin bugünkü kredi bütçesi doldu");
            }
            return new Admission(plan, o.reservation(), teamTokens);
        } catch (DataAccessException e) {
            // Bütçe Redis'i yok: toplu işler durur (fail-closed), sohbetler devam eder ama defterde izlenir.
            if (ctx.appType() == AppType.BATCH) {
                limiter.refund("team:" + ctx.team(), team.tokensPerMinute(), teamTokens);
                throw new GatewayException(HttpStatus.SERVICE_UNAVAILABLE, "budget_unavailable",
                        "Bütçe servisine ulaşılamıyor; toplu işler bekletiliyor");
            }
            log.warn("Bütçe kontrol edilemedi, sohbet isteği bütçesiz geçiyor: {}", e.getMessage());
            meters.counter("llmgw.budget.unenforced", "team", ctx.team()).increment();
            return new Admission(plan, null, teamTokens);
        }
    }

    /** En fazla tutar: zincirdeki EN PAHALI aday için (fallback'te bütçe aşılmasın). */
    private BudgetService.Outcome reserve(Prepared p, Router.Plan plan) {
        long max = 0;
        for (String alias : plan.candidates()) {
            ModelProperties m = props.models().get(alias);
            max = Math.max(max, CostCalculator.maxCostMicro(m, p.inputTokens(), maxTokens(m, p.req())));
        }
        return budget.reserve(p.ctx().team(), p.ctx().userId(), p.requestId(), max);
    }

    static int maxTokens(ModelProperties m, ChatCompletionRequest req) {
        int requested = req.maxTokens() == null ? m.defaultMaxTokens() : req.maxTokens();
        return Math.min(requested, m.maxOutputTokens());
    }

    // ------------------------------------------------------------------ normal (stream'siz) istek

    public ChatCompletionResponse complete(ClientContext ctx, ChatCompletionRequest req) {
        Prepared p0 = prepare(ctx, req);
        Router.Plan plan = Router.route(props, req.model(), ctx.confidential(), p0.inputTokens(), p0.maskedLastUser());
        Prepared p = withCacheKeys(p0, plan);

        Optional<Hit> hit = lookupCache(p);
        if (hit.isPresent()) {
            return cacheHitResponse(p, plan, hit.get());
        }
        Admission adm = admit(p, plan);
        try {
            AttemptResult r = callWithFallback(p, adm.plan());
            return finish(p, adm, r.attempt(), r.reply());
        } catch (GatewayException e) {
            release(p, adm, null, "failed", "");
            throw e;
        }
    }

    record AttemptResult(Attempt attempt, ProviderReply reply) {
    }

    private AttemptResult callWithFallback(Prepared p, Router.Plan plan) {
        Exception last = null;
        for (String alias : plan.candidates()) {
            Optional<Attempt> a = tryStart(p, plan, alias);
            if (a.isEmpty()) {
                continue;
            }
            Attempt at = a.get();
            CircuitBreaker cb = providers.breaker(at.provider());
            long t0 = System.nanoTime();
            try {
                ProviderReply reply = providers.get(at.provider()).call(request(p, at));
                cb.onSuccess(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
                return new AttemptResult(at, reply);
            } catch (Exception e) {
                cb.onError(System.nanoTime() - t0, TimeUnit.NANOSECONDS, e);
                refundProvider(at, at.providerTokens());
                last = e;
                log.warn("Sağlayıcı {} ({}) hata verdi, sıradakine geçiliyor: {}", at.provider(), alias, e.getMessage());
                meters.counter("llmgw.provider.error", "provider", at.provider()).increment();
            }
        }
        throw new GatewayException(HttpStatus.SERVICE_UNAVAILABLE, "all_providers_failed",
                "Hiçbir sağlayıcı cevap veremedi" + (last == null ? " (hepsi devre dışı ya da kapasitesi dolu)" : ": " + last.getMessage()));
    }

    /** Circuit breaker açık ya da sağlayıcı kapasitesi doluysa bu aday atlanır (yedeğin de boğulmaması için). */
    private Optional<Attempt> tryStart(Prepared p, Router.Plan plan, String alias) {
        ModelProperties m = props.models().get(alias);
        ProviderProperties pp = props.providers().get(m.provider());
        CircuitBreaker cb = providers.breaker(m.provider());
        if (!cb.tryAcquirePermission()) {
            meters.counter("llmgw.provider.skipped", "provider", m.provider(), "reason", "circuit_open").increment();
            return Optional.empty();
        }
        long tokens = p.inputTokens() + (long) maxTokens(m, p.req());
        if (!limiter.tryConsume("provider:" + m.provider(), pp.tokensPerMinute(), tokens).allowed()) {
            cb.releasePermission();
            meters.counter("llmgw.provider.skipped", "provider", m.provider(), "reason", "capacity").increment();
            return Optional.empty();
        }
        return Optional.of(new Attempt(alias, m, m.provider(), tokens, alias.equals(plan.primary()) ? null : plan.primary()));
    }

    private ProviderRequest request(Prepared p, Attempt a) {
        return new ProviderRequest(a.model().providerModel(), p.masked(), maxTokens(a.model(), p.req()),
                p.req().temperature(), p.promptCache());
    }

    private ChatCompletionResponse finish(Prepared p, Admission adm, Attempt at, ProviderReply reply) {
        String maskedAnswer = reply.text() == null ? "" : reply.text();
        TokenUsage usage = reply.usage() != null ? reply.usage()
                : TokenUsage.of(p.inputTokens(), TokenEstimator.estimate(maskedAnswer));
        long cost = settle(p, adm, at, usage);
        boolean complete = reply.finishReason() == null || !"length".equalsIgnoreCase(reply.finishReason());
        if (complete) {
            writeCaches(p, at.alias(), maskedAnswer, usage);
        }
        record(p, at, "MISS", usage, cost, null, "ok", maskedAnswer);
        String answer = p.vault().unmask(maskedAnswer);
        return response(p, at.alias(), answer, usage, reply.finishReason() == null ? "stop" : reply.finishReason(),
                new ChatCompletionResponse.GatewayInfo("MISS", at.alias(), at.provider(), at.fallbackFrom(),
                        CostCalculator.toCredits(cost), p.promptCache(), p.vault().size()));
    }

    private ChatCompletionResponse cacheHitResponse(Prepared p, Router.Plan plan, Hit hit) {
        meters.counter("llmgw.cache", "result", hit.status()).increment();
        CachedAnswer a = hit.answer();
        TokenUsage none = TokenUsage.of(0, 0);
        Attempt pseudo = new Attempt(plan.primary(), props.models().get(plan.primary()), null, 0, null);
        record(p, pseudo, hit.status(), none, 0, null, "ok", a.content());
        return response(p, plan.primary(), p.vault().unmask(a.content()), TokenUsage.of(a.promptTokens(), a.completionTokens()),
                "stop", new ChatCompletionResponse.GatewayInfo(hit.status(), plan.primary(), null, null, 0.0, false,
                        p.vault().size()));
    }

    // ------------------------------------------------------------------ stream

    /** Stream durumu: hangi aday deneniyor, ne kadar metin geldi, usage, bir kez kapatıldı mı. */
    static final class StreamState {
        final AtomicReference<Attempt> current = new AtomicReference<>();
        final StringBuilder maskedText = new StringBuilder();
        final AtomicReference<TokenUsage> usage = new AtomicReference<>();
        final AtomicReference<String> finishReason = new AtomicReference<>();
        final AtomicBoolean closed = new AtomicBoolean();
        volatile Long ttftMs;
    }

    /** SSE {@code data:} satırları (OpenAI chunk formatı), sonda {@code [DONE]}. */
    public Flux<String> stream(ClientContext ctx, ChatCompletionRequest req) {
        Prepared p0 = prepare(ctx, req);
        Router.Plan plan = Router.route(props, req.model(), ctx.confidential(), p0.inputTokens(), p0.maskedLastUser());
        Prepared p = withCacheKeys(p0, plan);

        Optional<Hit> hit = lookupCache(p);
        if (hit.isPresent()) {
            ChatCompletionResponse r = cacheHitResponse(p, plan, hit.get());
            String content = r.choices().getFirst().message().content();
            return Flux.just(chunk(p, plan.primary(), content, null, null, null), chunk(p, plan.primary(), null, "stop",
                    r.usage(), r.gateway()), "[DONE]");
        }
        // Rezervasyon ve limitler stream BAŞLAMADAN: reddedilirse istemci düzgün bir HTTP hatası alır.
        Admission adm = admit(p, plan);
        StreamState st = new StreamState();
        StreamingUnmasker un = new StreamingUnmasker(p.vault());

        Flux<String> body = attemptStream(p, adm.plan(), 0, st).concatMap(r -> {
            if (r.usage() != null) {
                st.usage.set(r.usage());
            }
            if (r.finishReason() != null) {
                st.finishReason.set(r.finishReason());
            }
            String text = r.text() == null ? "" : r.text();
            if (text.isEmpty()) {
                return Flux.empty();
            }
            if (st.ttftMs == null) {
                st.ttftMs = (System.nanoTime() - p.startNanos()) / 1_000_000;
            }
            st.maskedText.append(text);
            String out = un.accept(text);
            return out.isEmpty() ? Flux.empty() : Flux.just(chunk(p, st.current.get().alias(), out, null, null, null));
        });

        Flux<String> tail = Flux.defer(() -> {
            String rest = un.flush();
            ChatCompletionResponse.Usage u = finishStream(p, adm, st);
            Attempt at = st.current.get();
            ChatCompletionResponse.GatewayInfo gi = new ChatCompletionResponse.GatewayInfo("MISS", at.alias(), at.provider(),
                    at.fallbackFrom(), CostCalculator.toCredits(lastCost(st)), p.promptCache(), p.vault().size());
            Flux<String> restChunk = rest.isEmpty() ? Flux.empty() : Flux.just(chunk(p, at.alias(), rest, null, null, null));
            String finish = st.finishReason.get() == null ? "stop" : st.finishReason.get();
            return restChunk.concatWith(Flux.just(chunk(p, at.alias(), null, finish, u, gi), "[DONE]"));
        });

        return body.concatWith(tail)
                .onErrorResume(e -> Flux.defer(() -> {
                    String partial = un.flush();
                    abortStream(p, adm, st, e);
                    Flux<String> rest = partial.isEmpty() || st.current.get() == null ? Flux.empty()
                            : Flux.just(chunk(p, st.current.get().alias(), partial, null, null, null));
                    return rest.concatWith(Flux.just(errorChunk(e), "[DONE]"));
                }))
                .doOnCancel(() -> abortStream(p, adm, st, new IllegalStateException("client_cancelled")));
    }

    /**
     * Fallback sadece İLK token'dan önce: kullanıcıya bir şey gittiyse yarım cevabın üstüne başka modelin cevabı
     * eklenemez (zorluk #2). İlk token belli sürede gelmezse de yedeğe geçilir.
     */
    private Flux<ProviderReply> attemptStream(Prepared p, Router.Plan plan, int idx, StreamState st) {
        return Flux.defer(() -> {
            if (idx >= plan.candidates().size()) {
                return Flux.error(new GatewayException(HttpStatus.SERVICE_UNAVAILABLE, "all_providers_failed",
                        "Hiçbir sağlayıcı cevap veremedi"));
            }
            Optional<Attempt> a = tryStart(p, plan, plan.candidates().get(idx));
            if (a.isEmpty()) {
                return attemptStream(p, plan, idx + 1, st);
            }
            Attempt at = a.get();
            st.current.set(at);
            CircuitBreaker cb = providers.breaker(at.provider());
            AtomicBoolean started = new AtomicBoolean();
            long t0 = System.nanoTime();
            return providers.get(at.provider()).stream(request(p, at))
                    .timeout(Mono.delay(props.routing().firstTokenTimeout()), x -> Flux.never())
                    .doOnNext(r -> {
                        if (r.text() != null && !r.text().isEmpty()) {
                            started.set(true);
                        }
                    })
                    .doOnComplete(() -> cb.onSuccess(System.nanoTime() - t0, TimeUnit.NANOSECONDS))
                    .onErrorResume(e -> {
                        cb.onError(System.nanoTime() - t0, TimeUnit.NANOSECONDS, e);
                        meters.counter("llmgw.provider.error", "provider", at.provider()).increment();
                        if (started.get()) {
                            return Flux.error(new MidStreamException(at.alias(), e));
                        }
                        refundProvider(at, at.providerTokens());
                        log.warn("Stream başlamadan {} hata verdi, yedeğe geçiliyor: {}", at.provider(), e.toString());
                        return attemptStream(p, plan, idx + 1, st);
                    });
        });
    }

    static final class MidStreamException extends RuntimeException {
        MidStreamException(String alias, Throwable cause) {
            super(alias + " stream ortasında koptu: " + cause.getMessage(), cause);
        }
    }

    private final Map<StreamState, Long> streamCosts = java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    private long lastCost(StreamState st) {
        return streamCosts.getOrDefault(st, 0L);
    }

    private ChatCompletionResponse.Usage finishStream(Prepared p, Admission adm, StreamState st) {
        Attempt at = st.current.get();
        String masked = st.maskedText.toString();
        TokenUsage usage = st.usage.get() != null ? st.usage.get()
                : TokenUsage.of(p.inputTokens(), TokenEstimator.estimate(masked));
        if (st.closed.compareAndSet(false, true)) {
            long cost = settle(p, adm, at, usage);
            streamCosts.put(st, cost);
            if (!"length".equalsIgnoreCase(String.valueOf(st.finishReason.get()))) {
                writeCaches(p, at.alias(), masked, usage);
            }
            record(p, at, "MISS", usage, cost, st.ttftMs, "ok", masked);
        }
        return usageDto(usage);
    }

    /** Kopma ya da iptal: o ana kadar üretilen kadar ödenir (usage yoksa tahminle), cache'e yazılmaz. */
    private void abortStream(Prepared p, Admission adm, StreamState st, Throwable e) {
        if (!st.closed.compareAndSet(false, true)) {
            return;
        }
        Attempt at = st.current.get();
        String masked = st.maskedText.toString();
        if (at == null) {
            release(p, adm, null, "failed", "");
            return;
        }
        TokenUsage usage = st.usage.get() != null ? st.usage.get()
                : TokenUsage.of(p.inputTokens(), TokenEstimator.estimate(masked));
        long cost = settle(p, adm, at, usage);
        String status = masked.isEmpty() ? "failed" : "partial";
        record(p, at, "MISS", usage, cost, st.ttftMs, status, masked);
        log.warn("Stream kapandı ({}): {}", status, e.toString());
    }

    // ------------------------------------------------------------------ mutabakat, cache, defter

    private long settle(Prepared p, Admission adm, Attempt at, TokenUsage usage) {
        long cost = CostCalculator.actualCostMicro(at.model(), usage);
        if (adm.reservation() != null) {
            try {
                budget.settle(adm.reservation(), cost);
            } catch (DataAccessException e) {
                log.warn("Mutabakat yapılamadı, reaper kapatacak: {}", e.getMessage());
            }
        }
        long used = usage.totalInput() + (long) usage.output();
        TeamProperties team = props.teams().get(p.ctx().team());
        limiter.refund("team:" + p.ctx().team(), team.tokensPerMinute(), adm.teamTokens() - used);
        refundProvider(at, at.providerTokens() - used);
        meters.counter("llmgw.cost.micro", "team", p.ctx().team(), "model", at.alias()).increment(cost);
        return cost;
    }

    /** Sağlayıcıya hiç gidilemediyse rezervasyonun tamamı iade edilir. */
    private void release(Prepared p, Admission adm, Attempt at, String status, String masked) {
        if (adm.reservation() != null) {
            try {
                budget.settle(adm.reservation(), 0);
            } catch (DataAccessException e) {
                log.warn("Rezervasyon iade edilemedi, reaper kapatacak: {}", e.getMessage());
            }
        }
        TeamProperties team = props.teams().get(p.ctx().team());
        limiter.refund("team:" + p.ctx().team(), team.tokensPerMinute(), adm.teamTokens());
        Attempt pseudo = at != null ? at : new Attempt(adm.plan().primary(), props.models().get(adm.plan().primary()), null, 0, null);
        record(p, pseudo, "MISS", TokenUsage.of(0, 0), 0, null, status, masked);
    }

    private void refundProvider(Attempt at, long tokens) {
        if (at.provider() == null || tokens <= 0) {
            return;
        }
        limiter.refund("provider:" + at.provider(), props.providers().get(at.provider()).tokensPerMinute(), tokens);
    }

    private void writeCaches(Prepared p, String alias, String maskedAnswer, TokenUsage usage) {
        if (maskedAnswer.isEmpty()) {
            return;
        }
        CachedAnswer a = new CachedAnswer(maskedAnswer, alias, usage.totalInput(), usage.output());
        if (p.exactKey() != null) {
            exactCache.put(p.exactKey(), a, props.cache().exactTtl());
        }
        if (p.semanticScope() != null) {
            semanticCache.put(p.semanticScope(), p.maskedLastUser(), a);
        }
    }

    private void record(Prepared p, Attempt at, String cacheStatus, TokenUsage u, long cost, Long ttft, String status,
                        String maskedResponse) {
        ledger.record(new UsageEvent(p.requestId(), clock.instant(), p.ctx().team(), p.ctx().app(), p.ctx().userId(),
                p.req().model(), at.alias(), at.provider(), at.fallbackFrom(), cacheStatus,
                p.promptCache() && "MISS".equals(cacheStatus),
                u.uncachedInput(), u.cacheRead(), u.cacheWrite(), u.output(), cost,
                (System.nanoTime() - p.startNanos()) / 1_000_000, ttft, status, p.vault().size(),
                p.maskedLastUser(), maskedResponse));
    }

    // ------------------------------------------------------------------ cevap biçimleri

    private ChatCompletionResponse response(Prepared p, String alias, String content, TokenUsage usage, String finish,
                                            ChatCompletionResponse.GatewayInfo gi) {
        return new ChatCompletionResponse(p.requestId(), "chat.completion", p.created(), alias,
                List.of(new ChatCompletionResponse.Choice(0, new Message("assistant", content), finish)),
                usageDto(usage), gi);
    }

    private static ChatCompletionResponse.Usage usageDto(TokenUsage u) {
        return new ChatCompletionResponse.Usage(u.totalInput(), u.output(), u.totalInput() + u.output(),
                new ChatCompletionResponse.PromptDetails(u.cacheRead(), u.cacheWrite()));
    }

    private String chunk(Prepared p, String alias, String content, String finish, ChatCompletionResponse.Usage usage,
                         ChatCompletionResponse.GatewayInfo gi) {
        Map<String, Object> delta = new LinkedHashMap<>();
        if (content != null) {
            delta.put("content", content);
        }
        Map<String, Object> choice = new LinkedHashMap<>();
        choice.put("index", 0);
        choice.put("delta", delta);
        choice.put("finish_reason", finish);
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("id", p.requestId());
        c.put("object", "chat.completion.chunk");
        c.put("created", p.created());
        c.put("model", alias);
        c.put("choices", List.of(choice));
        if (usage != null) {
            c.put("usage", usage);
        }
        if (gi != null) {
            c.put("x_gateway", gi);
        }
        return write(c);
    }

    private String errorChunk(Throwable e) {
        String code = e instanceof GatewayException g ? g.code()
                : e instanceof MidStreamException ? "stream_interrupted" : "provider_error";
        return write(Map.of("error", Map.of("code", code, "message", String.valueOf(e.getMessage()))));
    }

    private String write(Object o) {
        try {
            return json.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
