package io.paramix.redislab.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Repository;

/** "Yavaş veritabanı" yerine geçen sahte depo: her okuma 50 ms sürer ve sayılır (cache'in etkisi ölçülsün diye). */
@Repository
public class ProductRepository {

    private final Map<String, Product> rows = new ConcurrentHashMap<>();
    private final AtomicInteger reads = new AtomicInteger();

    public Optional<Product> findById(String id) {
        reads.incrementAndGet();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return Optional.ofNullable(rows.get(id));
    }

    public void save(Product p) {
        rows.put(p.id(), p);
    }

    public int reads() {
        return reads.get();
    }
}
