package io.paramix.provision;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

/**
 * N işi AYNI ANDA başlatır: hepsi bir başlangıç kapısında bekler, kapı açılınca birlikte koşar. Sırayla
 * gönderilen istekler yarış durumunu (race condition) hiç ortaya çıkarmaz; bu test aracı onu zorlar.
 */
public final class Concurrently {

    private Concurrently() {
    }

    public static <T> List<T> run(int n, IntFunction<T> task) {
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(n)) {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int idx = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return task.apply(idx);
                }));
            }
            ready.await();
            go.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get());
            }
            return results;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
