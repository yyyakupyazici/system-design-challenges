package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import io.paramix.redislab.cache.CacheAsideService;
import io.paramix.redislab.cache.Product;
import io.paramix.redislab.cache.ProductRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

class CacheAsideIT extends AbstractRedisIT {

    @Autowired CacheAsideService cache;
    @Autowired ProductRepository repo;
    @Autowired StringRedisTemplate redis;

    @Test
    void fiftyConcurrentMissesHitTheDatabaseOnlyOnce() throws Exception {
        String id = unique("p");
        repo.save(new Product(id, "Kulaklık", 129_900));
        int before = repo.reads();

        List<Callable<Optional<Product>>> calls = IntStream.range(0, 50).<Callable<Optional<Product>>>mapToObj(i -> () -> cache.get(id)).toList();
        List<Future<Optional<Product>>> results;
        try (ExecutorService pool = Executors.newFixedThreadPool(50)) {
            results = pool.invokeAll(calls);
        }
        for (Future<Optional<Product>> f : results) {
            assertThat(f.get()).map(Product::name).contains("Kulaklık");
        }
        assertThat(repo.reads() - before).isEqualTo(1);   // stampede yok
    }

    @Test
    void missingIdsAreCachedToo() {
        String id = unique("missing");
        int before = repo.reads();
        assertThat(cache.get(id)).isEmpty();
        assertThat(cache.get(id)).isEmpty();
        assertThat(repo.reads() - before).isEqualTo(1);   // ikinci sorgu DB'ye gitmedi (negative caching)
    }

    @Test
    void updateDeletesTheCacheEntrySoTheNextReadIsFresh() {
        String id = unique("p");
        repo.save(new Product(id, "Eski", 100));
        cache.get(id);
        cache.update(new Product(id, "Yeni", 200));
        assertThat(cache.get(id)).map(Product::name).contains("Yeni");
    }

    @Test
    void ttlHasRandomJitterSoKeysDoNotExpireTogether() {
        String id = unique("p");
        repo.save(new Product(id, "X", 1));
        cache.get(id);
        Long ttl = redis.getExpire("product:{" + id + "}");
        assertThat(ttl).isBetween(Duration.ofMinutes(10).toSeconds() - 2, Duration.ofMinutes(12).toSeconds());
    }
}
