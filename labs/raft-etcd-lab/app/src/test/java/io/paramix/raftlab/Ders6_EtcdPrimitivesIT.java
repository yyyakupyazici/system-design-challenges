package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 6: Patroni'nin etcd'den kullandığı üç araç. Hepsi Raft üzerinde çalışır.
 *
 * <pre>
 *  LEASE   süreli kira: yenilenmezse ona bağlı anahtarlar KENDİLİĞİNDEN silinir     → "lider öldü" bilgisi
 *  TXN     "şu doğruysa şunu yaz", tek atomik adım (compare-and-set)                 → aynı anda tek lider
 *  WATCH   anahtar değişince haber ver                                                → sormadan öğren
 *  + REVISION: her yazmada artan sayı                                                 → fencing token
 * </pre>
 */
class Ders6_EtcdPrimitivesIT {

    private static Client client;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        Lab.fresh();
        client = Client.builder().endpoints(Lab.ENDPOINTS.split(",")).build();
    }

    @AfterAll
    static void down() {
        if (client != null) {
            client.close();
            Lab.down();
        }
    }

    private static ByteSequence bs(String s) {
        return EtcdProbe.bs(s);
    }

    @Test
    void leaseKeyDisappearsWhenNotRenewedAndStaysWhileRenewed() throws Exception {
        Lab.say("LEASE: 3 sn'lik kira al, iki anahtar bağla: birini yenile, ötekini bırak");
        long renewed = client.getLeaseClient().grant(3).get().getID();
        long abandoned = client.getLeaseClient().grant(3).get().getID();
        client.getKVClient().put(bs("/ders/lider-a"), bs("ben"), PutOption.builder().withLeaseId(renewed).build()).get();
        client.getKVClient().put(bs("/ders/lider-b"), bs("ben"), PutOption.builder().withLeaseId(abandoned).build()).get();

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 6_000) {
            client.getLeaseClient().keepAliveOnce(renewed).get();               // sadece a'nın kirası uzatılıyor
            Thread.sleep(1_000);
        }
        assertThat(client.getKVClient().get(bs("/ders/lider-a")).get().getKvs()).as("yenilenen kalır").hasSize(1);
        assertThat(client.getKVClient().get(bs("/ders/lider-b")).get().getKvs()).as("yenilenmeyen silinir").isEmpty();
        Lab.say("6 sn sonra: lider-a duruyor (kirası uzatıldı), lider-b SİLİNDİ (kimse uzatmadı)");
    }

    @Test
    void compareAndSetOnlyOneOfTenCandidatesWins() throws Exception {
        Lab.say("TXN: 10 aday aynı anda 'lider anahtarı YOKSA beni yaz' diyor");
        ByteSequence key = bs("/ders/cas-lider");
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<Callable<Boolean>> candidates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String me = "aday-" + i;
            candidates.add(() -> client.getKVClient().txn()
                    .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0)))      // createRevision 0 = anahtar yok
                    .Then(Op.put(key, bs(me), PutOption.DEFAULT))
                    .commit().get().isSucceeded());
        }
        int winners = 0;
        for (Future<Boolean> f : pool.invokeAll(candidates)) {
            winners += f.get() ? 1 : 0;
        }
        pool.shutdown();
        String winner = client.getKVClient().get(key).get().getKvs().get(0).getValue().toString(java.nio.charset.StandardCharsets.UTF_8);
        Lab.say("kazanan sayısı: %d (%s). Raft bütün txn'leri tek sıraya dizdi: ilki yazdı, sonrakiler 'anahtar var' gördü",
                winners, winner);
        assertThat(winners).isEqualTo(1);
    }

    @Test
    void watchHearsTheLeaderKeyDisappearWithoutPolling() throws Exception {
        Lab.say("WATCH: lider anahtarını izle; kira dolunca silinme olayı GELİR (sormaya gerek yok)");
        long lease = client.getLeaseClient().grant(2).get().getID();
        ByteSequence key = bs("/ders/izlenen-lider");
        client.getKVClient().put(key, bs("pg-ist"), PutOption.builder().withLeaseId(lease).build()).get();
        List<String> heard = new CopyOnWriteArrayList<>();
        long start = System.currentTimeMillis();
        try (Watch.Watcher w = client.getWatchClient().watch(key, Watch.listener(r -> r.getEvents()
                .forEach(e -> heard.add(e.getEventType() + " @" + (System.currentTimeMillis() - start) + "ms"))))) {
            Awaitility.await().atMost(Duration.ofSeconds(8)).until(() -> heard.stream().anyMatch(s -> s.startsWith(WatchEvent.EventType.DELETE.name())));
        }
        Lab.say("duyulan: %s (kira 2 sn'di)", heard);
        assertThat(heard).anyMatch(s -> s.startsWith("DELETE"));
    }

    @Test
    void revisionGrowsWithEveryWriteAndWorksAsAFencingToken() throws Exception {
        Lab.say("REVISION: kümedeki her yazma sayacı artırır. Lider olduğun andaki revision = fencing token");
        ByteSequence key = bs("/ders/fencing-lider");
        long r1 = client.getKVClient().put(key, bs("pg-ist")).get().getHeader().getRevision();
        client.getKVClient().delete(key).get();
        long r2 = client.getKVClient().put(key, bs("pg-ank")).get().getHeader().getRevision();
        Lab.say("pg-ist lider olduğunda revision %d, pg-ank lider olduğunda %d. Depo 'en büyük token'ı kabul ederse"
                + " eski liderin geç kalan yazması (token %d) reddedilir", r1, r2, r1);
        assertThat(r2).isGreaterThan(r1);
        TimeUnit.MILLISECONDS.sleep(1);
    }
}
