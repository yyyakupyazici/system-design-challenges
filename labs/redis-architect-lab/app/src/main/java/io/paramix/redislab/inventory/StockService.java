package io.paramix.redislab.inventory;

import java.util.List;

import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Flash sale: 100 ürün, 50.000 kişi aynı saniyede "al" diyor. Veritabanında tek bir stok satırını güncellemek
 * satır kilidinde tıkanır (006'da ölçtük: tek satırın tavanı ≈ 1 / kilit süresi). Redis'te stok düşmek
 * mikrosaniyeler sürer ve Lua sayesinde "stok yetiyorsa düş" asla eksiye inmez.
 *
 * <p>Akış: Redis'te rezerve et → kullanıcı ödemeye gider → ödeme başarılıysa sipariş DB'ye yazılır,
 * başarısızsa ya da süre dolarsa {@link #release} ile stok geri döner. Gün sonu DB ile mutabakat yapılır:
 * Redis hızlı kapı, DB gerçeğin kaynağı.
 */
@Service
public class StockService {

    public enum Result { RESERVED, SOLD_OUT, ALREADY_RESERVED }

    private static final RedisScript<Long> RESERVE = Scripts.load("stock_reserve", Long.class);
    private static final RedisScript<Long> RELEASE = Scripts.load("stock_release", Long.class);

    private final StringRedisTemplate redis;

    public StockService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    static List<String> keys(String sku) {
        return List.of("stock:{" + sku + "}", "stock:{" + sku + "}:orders");
    }

    public void setStock(String sku, long qty) {
        redis.opsForValue().set(keys(sku).getFirst(), String.valueOf(qty));
    }

    public Result reserve(String sku, String orderId, int qty) {
        long r = redis.execute(RESERVE, keys(sku), orderId, String.valueOf(qty));
        return r == 1 ? Result.RESERVED : r == 2 ? Result.ALREADY_RESERVED : Result.SOLD_OUT;
    }

    public long release(String sku, String orderId) {
        Long r = redis.execute(RELEASE, keys(sku), orderId);
        return r == null ? 0 : r;
    }

    public long remaining(String sku) {
        String v = redis.opsForValue().get(keys(sku).getFirst());
        return v == null ? 0 : Long.parseLong(v);
    }

    public long reservedOrders(String sku) {
        return redis.opsForHash().size(keys(sku).get(1));
    }
}
