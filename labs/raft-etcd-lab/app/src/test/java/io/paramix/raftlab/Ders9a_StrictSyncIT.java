package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.paramix.raftlab.etcd.EtcdStore;
import io.paramix.raftlab.ledger.LedgerService;
import io.paramix.raftlab.ledger.LedgerService.TransferResult;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 9a: strict senkronun bedeli. Senkron standby cevap vermezse commit "tamam" ALAMAZ, BEKLER.
 *
 * <pre>
 *  ① pg-ank (senkron standby) donar
 *  ② transfer başlar: pg-ist kendi diskine yazdı ama pg-ank'in onayını bekliyor → istemci cevap alamaz
 *  ③ wal_sender_timeout (5 sn) sonra pg-ank düşer → ajan Ankara'daki diğer kopyayı (pg-ank2) senkron yapar
 *     (önce /sync genişler, sonra synchronous_standby_names değişir, sonra /sync daralır)
 *  ④ bekleyen commit pg-ank2'nin onayıyla "tamam" alır. RPO 0 hiç bozulmadı, sadece birkaç saniye beklendi
 * </pre>
 */
class Ders9a_StrictSyncIT {

    private static Agents agents;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        agents = PgLabSupport.startSteadyCluster("SINGLE", Lab.ENDPOINTS, Lab.ENDPOINTS);
    }

    @AfterAll
    static void down() {
        if (agents != null) {
            agents.printTimeline();
            agents.close();
            Lab.down();
        }
    }

    @Test
    void commitWaitsForTheSyncStandbyAndContinuesWhenAnotherCopyTakesOver() throws Exception {
        EtcdStore etcd = agents.bean("pg-ist", EtcdStore.class);
        assertThat(etcd.sync()).containsExactly("pg-ank");

        Lab.say("① Senkron standby pg-ank donuyor");
        Lab.pause("pg-ank");
        try {
            long start = System.currentTimeMillis();
            Lab.say("② Transfer başlıyor: pg-ist, pg-ank'in 'WAL diskimde' demesini bekleyecek");
            CompletableFuture<TransferResult> f = CompletableFuture.supplyAsync(() -> {
                try {
                    return LedgerService.transferOn("127.0.0.1", 5511, "pg-ist", "ali", "ayse", 7_700);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            Lab.sleep(Duration.ofSeconds(2));
            assertThat(f).as("2 sn sonra hâlâ 'tamam' yok").isNotDone();
            Lab.say("   2 sn geçti: commit hâlâ bekliyor (istemci cevap almadı)");

            TransferResult r = f.get(20, TimeUnit.SECONDS);
            long waited = System.currentTimeMillis() - start;
            Lab.say("③④ %d ms sonra 'tamam': ajan pg-ank2'yi senkron yaptı, /sync = %s", waited, etcd.sync());
            assertThat(etcd.sync()).containsExactly("pg-ank2");
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                    ((Number) Lab.query("pg-ank2", "SELECT count(*) FROM transfers WHERE id = " + r.transferId())).longValue() == 1);
            Lab.say("   transfer %d pg-ank2'de: 'tamam' denen her şey yine başka bölgede", r.transferId());
        } finally {
            Lab.resume("pg-ank");
        }
        System.out.println(Lab.run("status"));
    }
}
