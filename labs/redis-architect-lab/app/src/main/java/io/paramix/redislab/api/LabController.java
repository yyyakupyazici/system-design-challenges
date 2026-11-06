package io.paramix.redislab.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.redislab.atomic.CounterService;
import io.paramix.redislab.cache.CacheAsideService;
import io.paramix.redislab.cache.Product;
import io.paramix.redislab.cluster.ClusterInspector;
import io.paramix.redislab.delayedqueue.DelayedJobQueue;
import io.paramix.redislab.idempotency.IdempotencyService;
import io.paramix.redislab.inventory.StockService;
import io.paramix.redislab.leaderboard.LeaderboardService;
import io.paramix.redislab.lock.RedisLock;
import io.paramix.redislab.probabilistic.AnalyticsService;
import io.paramix.redislab.ratelimit.Decision;
import io.paramix.redislab.ratelimit.RateLimiters;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Her senaryo için elle denenebilecek uçlar (k8s'te port-forward ile). Örnekler README'de.
 * Asıl öğretici kısım servisler ve testler; bu sınıf sadece onları HTTP'ye açar.
 */
@RestController
@RequestMapping("/lab")
public class LabController {

    private final CounterService counters;
    private final CacheAsideService cache;
    private final RedisLock locks;
    private final RateLimiters limiters;
    private final IdempotencyService idempotency;
    private final StockService stock;
    private final LeaderboardService leaderboard;
    private final DelayedJobQueue queue;
    private final AnalyticsService analytics;
    private final ClusterInspector cluster;

    public LabController(CounterService counters, CacheAsideService cache, RedisLock locks, RateLimiters limiters,
                         IdempotencyService idempotency, StockService stock, LeaderboardService leaderboard,
                         DelayedJobQueue queue, AnalyticsService analytics, ClusterInspector cluster) {
        this.counters = counters;
        this.cache = cache;
        this.locks = locks;
        this.limiters = limiters;
        this.idempotency = idempotency;
        this.stock = stock;
        this.leaderboard = leaderboard;
        this.queue = queue;
        this.analytics = analytics;
        this.cluster = cluster;
    }

    @PostMapping("/counters/{name}")
    public Map<String, Object> increment(@PathVariable String name) {
        return Map.of("value", counters.atomicIncrement("counter:{" + name + "}"));
    }

    @GetMapping("/products/{id}")
    public ResponseEntity<Product> product(@PathVariable String id) {
        return cache.get(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/products/{id}")
    public Product saveProduct(@PathVariable String id, @RequestBody Product p) {
        Product saved = new Product(id, p.name(), p.priceKurus());
        cache.update(saved);
        return saved;
    }

    @PostMapping("/jobs/{name}/run")
    public ResponseEntity<Map<String, Object>> runExclusive(@PathVariable String name) {
        return locks.withLock("job:" + name, Duration.ofSeconds(30), h -> Map.<String, Object>of("ranWithFence", h.fencingToken()))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "başka bir pod çalıştırıyor")));
    }

    @GetMapping("/ratelimit/{algo}/{client}")
    public ResponseEntity<Decision> limit(@PathVariable String algo, @PathVariable String client) {
        Decision d = switch (algo) {
            case "fixed" -> limiters.fixedWindow(client, 10, Duration.ofSeconds(10));
            case "sliding" -> limiters.slidingLog(client, 10, Duration.ofSeconds(10));
            default -> limiters.tokenBucket(client, 10, 1.0);
        };
        return ResponseEntity.status(d.allowed() ? HttpStatus.OK : HttpStatus.TOO_MANY_REQUESTS).body(d);
    }

    @PostMapping("/payments")
    public ResponseEntity<Map<String, Object>> pay(@RequestHeader("Idempotency-Key") String key,
                                                   @RequestParam long amountKurus) {
        IdempotencyService.Outcome o = idempotency.execute(key, Duration.ofHours(24),
                () -> "pay_" + UUID.randomUUID() + "_" + amountKurus);
        return switch (o) {
            case IdempotencyService.Executed e -> ResponseEntity.status(HttpStatus.CREATED).body(Map.of("payment", e.result()));
            case IdempotencyService.Replayed r -> ResponseEntity.ok(Map.of("payment", r.result(), "replayed", true));
            case IdempotencyService.InProgress p -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "işleniyor"));
        };
    }

    @PutMapping("/stock/{sku}")
    public Map<String, Object> setStock(@PathVariable String sku, @RequestParam long qty) {
        stock.setStock(sku, qty);
        return Map.of("sku", sku, "remaining", qty);
    }

    @PostMapping("/stock/{sku}/reserve")
    public ResponseEntity<Map<String, Object>> reserve(@PathVariable String sku, @RequestParam String orderId,
                                                       @RequestParam(defaultValue = "1") int qty) {
        StockService.Result r = stock.reserve(sku, orderId, qty);
        HttpStatus status = r == StockService.Result.SOLD_OUT ? HttpStatus.CONFLICT : HttpStatus.OK;
        return ResponseEntity.status(status).body(Map.of("result", r, "remaining", stock.remaining(sku)));
    }

    @PostMapping("/leaderboard/{board}/{member}")
    public Map<String, Object> score(@PathVariable String board, @PathVariable String member, @RequestParam long delta) {
        return Map.of("score", leaderboard.addScore(board, member, delta), "rank", leaderboard.rank(board, member));
    }

    @GetMapping("/leaderboard/{board}")
    public List<LeaderboardService.Entry> top(@PathVariable String board, @RequestParam(defaultValue = "10") int n) {
        return leaderboard.top(board, n);
    }

    @PostMapping("/queue/{q}")
    public Map<String, Object> schedule(@PathVariable String q, @RequestParam long delaySeconds, @RequestBody String payload) {
        String id = UUID.randomUUID().toString();
        queue.schedule(q, id, payload, Instant.now().plusSeconds(delaySeconds));
        return Map.of("id", id);
    }

    @PostMapping("/queue/{q}/claim")
    public List<DelayedJobQueue.Job> claim(@PathVariable String q) {
        return queue.claim(q, 10, 30_000);
    }

    @GetMapping("/couriers/{city}/near")
    public List<String> near(@PathVariable String city, @RequestParam double lon, @RequestParam double lat) {
        return analytics.nearestCouriers(city, lon, lat, 2, 5);
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster(@RequestParam(defaultValue = "user:42,cart:{user42},orders:{user42}") List<String> keys) {
        return Map.of("cluster", cluster.isCluster(), "nodes", cluster.topology(), "slots", cluster.slots(keys));
    }
}
