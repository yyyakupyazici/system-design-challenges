package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.redislab.inventory.StockService;
import io.paramix.redislab.inventory.StockService.Result;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class StockIT extends AbstractRedisIT {

    @Autowired StockService stock;

    @Test
    void threeHundredBuyersForAHundredItemsNeverOversell() throws Exception {
        String sku = unique("iphone");
        stock.setStock(sku, 100);
        AtomicInteger reserved = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(32)) {
            for (int i = 0; i < 300; i++) {
                String order = "order-" + i;
                pool.submit(() -> {
                    if (stock.reserve(sku, order, 1) == Result.RESERVED) {
                        reserved.incrementAndGet();
                    }
                });
            }
        }
        assertThat(reserved.get()).isEqualTo(100);
        assertThat(stock.remaining(sku)).isZero();
        assertThat(stock.reservedOrders(sku)).isEqualTo(100);
    }

    @Test
    void retryingTheSameOrderDoesNotTakeStockTwice() {
        String sku = unique("tv");
        stock.setStock(sku, 5);
        assertThat(stock.reserve(sku, "o-1", 2)).isEqualTo(Result.RESERVED);
        assertThat(stock.reserve(sku, "o-1", 2)).isEqualTo(Result.ALREADY_RESERVED);
        assertThat(stock.remaining(sku)).isEqualTo(3);
    }

    @Test
    void releaseReturnsTheItemsOnce() {
        String sku = unique("tv");
        stock.setStock(sku, 5);
        stock.reserve(sku, "o-1", 2);
        assertThat(stock.release(sku, "o-1")).isEqualTo(2);
        assertThat(stock.release(sku, "o-1")).isZero();
        assertThat(stock.remaining(sku)).isEqualTo(5);
    }
}
