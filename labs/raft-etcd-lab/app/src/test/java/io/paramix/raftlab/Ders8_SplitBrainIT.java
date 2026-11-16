package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.paramix.raftlab.ha.HaAgent.Role;
import io.paramix.raftlab.ledger.LedgerService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 8: split-brain. İstanbul'un etcd ile bağlantısı koptu ama İstanbul'daki PostgreSQL sapasağlam.
 * (Lab basitleştirmesi: her ajan sadece kendi bölgesinin etcd'sine bağlı; etcd-ist'i dondurmak
 * "İstanbul çoğunluktan koptu" demek. etcd-ank + etcd-wit hâlâ çoğunluk.)
 *
 * <pre>
 *  İki "uygulama" durmadan yazıyor: biri DOĞRUDAN pg-ist'e, biri DOĞRUDAN pg-ank'e (eski bağlantı bilgisiyle çalışan
 *  servisleri temsil ediyor). Soru: iki PostgreSQL'in AYNI ANDA yazma kabul ettiği bir an oluyor mu?
 *
 *  t=0        etcd-ist donar. pg-ist ajanı kirasını uzatamaz
 *  t≈ttl/2    pg-ist ajanı: "kiram her an düşebilir" → pg-ist'i YAZMAYA KAPATIR (fence)
 *  t≈ttl      kira düşer → pg-ank ajanı lider kaydını alır → promote
 *  Beklenen: pg-ist'e yapılan son başarılı yazma, pg-ank'e yapılan ilk başarılı yazmadan ÖNCE.
 * </pre>
 */
class Ders8_SplitBrainIT {

    private static Agents agents;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        agents = PgLabSupport.startSteadyCluster("SINGLE", Lab.ETCD_IST, Lab.ETCD_ANK);
    }

    @AfterAll
    static void down() {
        if (agents != null) {
            agents.printTimeline();
            agents.close();
            Lab.down();
        }
    }

    record Write(String node, long at, boolean ok, String error) {
    }

    @Test
    void neverTwoWritablePrimariesAtTheSameTime() throws Exception {
        List<Write> writes = new CopyOnWriteArrayList<>();
        AtomicBoolean running = new AtomicBoolean(true);
        Thread toIst = writer("pg-ist", writes, running);
        Thread toAnk = writer("pg-ank", writes, running);
        Lab.sleep(Duration.ofSeconds(2));

        Lab.say("t=0: etcd-ist donuyor → İstanbul çoğunluktan koptu. PostgreSQL'ler sağlam, iki yazıcı çalışıyor");
        long t0 = System.currentTimeMillis();
        Lab.pause("etcd-ist");
        try {
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> agents.agent("pg-ank").role() == Role.PRIMARY && writes.stream().anyMatch(w -> w.node().equals("pg-ank") && w.ok()));
            Lab.sleep(Duration.ofSeconds(2));
        } finally {
            running.set(false);
            toIst.join(15_000);
            toAnk.join(15_000);
        }

        long lastIst = writes.stream().filter(w -> w.node().equals("pg-ist") && w.ok()).mapToLong(Write::at).max().orElse(0);
        long firstAnk = writes.stream().filter(w -> w.node().equals("pg-ank") && w.ok()).mapToLong(Write::at).min().orElseThrow();
        Write firstIstFail = writes.stream().filter(w -> w.node().equals("pg-ist") && !w.ok() && w.at() > t0).findFirst().orElse(null);
        Lab.say("pg-ist'e son başarılı yazma: t=%+d ms", lastIst - t0);
        Lab.say("pg-ist ilk ret: t=%+d ms → %s", firstIstFail == null ? -1 : firstIstFail.at() - t0,
                firstIstFail == null ? "-" : firstIstFail.error());
        Lab.say("pg-ank'e ilk başarılı yazma: t=%+d ms", firstAnk - t0);
        Lab.say("Arada kimsenin yazamadığı boşluk: %d ms. İki primary'nin aynı anda yazdığı an: YOK", firstAnk - lastIst);
        assertThat(lastIst).as("eski primary, yeni primary yazmaya başlamadan ÖNCE durmuş olmalı").isLessThan(firstAnk);

        Lab.say("etcd-ist geri geliyor. pg-ist ajanı panoda 'lider pg-ank' görür: kendini AÇMAZ (eski primary)");
        Lab.resume("etcd-ist");
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> agents.agent("pg-ist").role() == Role.FENCED);
        Lab.sleep(Duration.ofSeconds(3));
        assertThat(agents.agent("pg-ist").role()).isEqualTo(Role.FENCED);
        assertThat(Lab.query("pg-ist", "SELECT current_setting('default_transaction_read_only')")).isEqualTo("on");
        System.out.println(Lab.run("status"));
    }

    private static Thread writer(String node, List<Write> writes, AtomicBoolean running) {
        return Thread.ofPlatform().name("writer-" + node).start(() -> {
            while (running.get()) {
                try {
                    LedgerService.transferOn("127.0.0.1", Lab.port(node), node, "ali", "ayse", 1);
                    writes.add(new Write(node, System.currentTimeMillis(), true, null));
                } catch (Exception e) {
                    String msg = String.valueOf(e.getMessage());
                    writes.add(new Write(node, System.currentTimeMillis(), false, msg.length() > 80 ? msg.substring(0, 80) : msg));
                }
                Lab.sleep(Duration.ofMillis(100));
            }
        });
    }
}
