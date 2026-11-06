package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.redislab.delayedqueue.DelayedJobQueue;
import io.paramix.redislab.delayedqueue.DelayedJobQueue.Job;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class DelayedQueueIT extends AbstractRedisIT {

    @Autowired DelayedJobQueue queue;

    @Test
    void onlyDueJobsAreClaimed() {
        String q = unique("hatirlatma");
        queue.schedule(q, "gecmis", "sepet hatırlatma", Instant.now().minusSeconds(1));
        queue.schedule(q, "gelecek", "yarınki hatırlatma", Instant.now().plusSeconds(3600));
        List<Job> jobs = queue.claim(q, 10, 30_000);
        assertThat(jobs).containsExactly(new Job("gecmis", "sepet hatırlatma"));
        assertThat(queue.readyCount(q)).isEqualTo(1);
    }

    @Test
    void competingWorkersNeverGetTheSameJob() throws Exception {
        String q = unique("bulk");
        for (int i = 0; i < 200; i++) {
            queue.schedule(q, "job-" + i, "p" + i, Instant.now().minusMillis(10));
        }
        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            for (int w = 0; w < 8; w++) {
                pool.submit(() -> {
                    List<Job> batch;
                    while (!(batch = queue.claim(q, 7, 30_000)).isEmpty()) {
                        batch.forEach(j -> {
                            if (!seen.add(j.id())) {
                                duplicates.incrementAndGet();
                            }
                            queue.ack(q, j.id());
                        });
                    }
                });
            }
        }
        assertThat(duplicates.get()).isZero();
        assertThat(seen).hasSize(200);
    }

    @Test
    void aJobWhoseWorkerDiedIsGivenOutAgain() throws Exception {
        String q = unique("odeme-iptal");
        queue.schedule(q, "rez-1", "15 dk doldu, iptal et", Instant.now().minusMillis(1));
        assertThat(queue.claim(q, 1, 100)).hasSize(1);          // worker aldı ve "öldü" (ack yok)
        assertThat(queue.claim(q, 1, 100)).isEmpty();           // görünürlük süresi boyunca kimse alamaz
        Thread.sleep(200);
        assertThat(queue.requeueExpired(q)).isEqualTo(1);       // reaper geri koydu
        assertThat(queue.claim(q, 1, 100)).extracting(Job::id).containsExactly("rez-1");
    }
}
