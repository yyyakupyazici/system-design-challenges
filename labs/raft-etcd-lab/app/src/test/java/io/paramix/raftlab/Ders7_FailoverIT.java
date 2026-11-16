package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.paramix.raftlab.ha.HaAgent.Role;
import io.paramix.raftlab.ledger.LedgerService;
import io.paramix.raftlab.ledger.LedgerService.TransferResult;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 7: İstanbul yanıyor. Hepsi birlikte: Raft (etcd çoğunluğu), lease (lider kaydı düşer),
 * compare-and-set (Ankara kaydı alır), senkron replikasyon (RPO 0), promote.
 *
 * <pre>
 *  ① 50 transfer, her biri "tamam" aldı (commit = pg-ank WAL'ı diskine yazdı)
 *  ② İstanbul gider: pg-ist + pg-ist2 + etcd-ist + iki ajan. Kalan: etcd-ank + etcd-wit = 2/3 ✓
 *  ③ Lider kaydının kirası ttl (6 sn) sonunda düşer → /sync'teki Ankara standby'ı kaydı alır → promote
 *  ④ "tamam" denen 50 transferin HEPSİ yeni liderde (RPO 0), para toplamı korunmuş
 *  ⑤ Yeni lider Ankara'daki ikinci kopyayı (pg-ank2) senkron yapar → yazmaya RPO 0 ile devam.
 *     Ankara'da ikinci kopya olmasaydı: strict modda commit'ler BEKLERDİ (tek kopyayla "tamam" demek RPO 0'ı bozar)
 * </pre>
 */
class Ders7_FailoverIT {

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
    void istanbulBurnsAnkaraTakesOverWithEveryAcknowledgedTransfer() throws Exception {
        LedgerService ledger = agents.bean("pg-ank", LedgerService.class);   // uygulama Ankara'da da çalışıyor

        Lab.say("① 50 transfer: her biri 'tamam' aldı");
        List<Long> acknowledged = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            TransferResult r = ledger.transfer("ali", "ayse", 1_000);
            assertThat(r.primary()).isEqualTo("pg-ist");
            acknowledged.add(r.transferId());
        }

        String heir = String.join("", agents.bean("pg-ist", io.paramix.raftlab.etcd.EtcdStore.class).sync());
        assertThat(heir).as("senkron standby başka bölgeden").startsWith("pg-ank");
        Lab.say("/sync = %s: 'tamam' denen her commit Ankara'daki bu düğümde", heir);

        Lab.say("② İstanbul yanıyor: pg-ist, pg-ist2, etcd-ist ve iki ajan aynı anda gidiyor");
        long burnedAt = System.currentTimeMillis();
        agents.stop("pg-ist");
        agents.stop("pg-ist2");
        Lab.kill("pg-ist");
        Lab.kill("pg-ist2");
        Lab.kill("etcd-ist");

        Lab.say("③ Ankara bekliyor: lider kaydının kirası düşmeli (ttl 6 sn), sonra compare-and-set + promote");
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> agents.agent(heir).role() == Role.PRIMARY);
        long rtoMs = System.currentTimeMillis() - burnedAt;
        String other = heir.equals("pg-ank") ? "pg-ank2" : "pg-ank";
        Lab.say("%s lider oldu: RTO ≈ %d ms (≈ ttl + bir döngü). %s /sync'te olmadığı için aday OLMADI", heir, rtoMs, other);
        System.out.println(Lab.run("status"));

        Lab.say("④ RPO 0 kontrolü: 'tamam' denen 50 transferin hepsi %s'de mi?", heir);
        long present = (long) Lab.query(heir, "SELECT count(*) FROM transfers WHERE id BETWEEN "
                + acknowledged.get(0) + " AND " + acknowledged.get(49));
        long total = ((Number) Lab.query(heir, "SELECT sum(balance) FROM accounts")).longValue();
        Lab.say("%s'de %d/50 transfer var, toplam bakiye %d (başta 10.000.000)", heir, present, total);
        assertThat(present).isEqualTo(50);
        assertThat(total).as("para yoktan var olmadı, kaybolmadı").isEqualTo(10_000_000L);
        assertThat(Lab.query(heir, "SELECT balance FROM accounts WHERE id = 'ayse'")).isEqualTo(50_000L);

        Lab.say("⑤ Yeni lider, Ankara'daki diğer kopyayı (%s) senkron yapınca yazmaya RPO 0 ile devam", other);
        TransferResult after = ledger.transfer("ali", "ayse", 1_000);
        assertThat(after.primary()).isEqualTo(heir);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                ((Number) Lab.query(other, "SELECT count(*) FROM transfers WHERE id = " + after.transferId())).longValue() == 1);
        Lab.say("yeni transfer %d: hem %s hem %s'de", after.transferId(), heir, other);
        System.out.println(Lab.run("status"));
        assertThat(rtoMs).isLessThan(20_000);
    }
}
