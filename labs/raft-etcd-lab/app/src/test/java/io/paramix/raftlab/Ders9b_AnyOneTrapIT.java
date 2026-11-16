package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;

import io.paramix.raftlab.ha.HaAgent.Role;
import io.paramix.raftlab.ledger.LedgerService;
import io.paramix.raftlab.ledger.LedgerService.TransferResult;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 9b: "ANY 1" tuzağı. synchronous_standby_names = ANY 1 ("pg-ank", "pg-ank2", "pg-ist2").
 * Commit, HERHANGİ bir standby onaylayınca "tamam" alır, aynı binadaki standby da olabilir.
 *
 * <pre>
 *  ① Ankara'nın iki standby'ı yavaş/donuk. Transfer "tamam" alır: onayı İstanbul'daki pg-ist2 verdi
 *  ② Hemen ardından İstanbul komple yanar (pg-ist + pg-ist2)
 *  ③ Ankara toparlanır, /sync'te adı olduğu için lider olur
 *  ④ "tamam" denen transfer YOK. RPO 0 bozuldu, ve sistem bunu fark etmedi bile.
 *  Ders: onay, felaketin götüremeyeceği bir yerden gelmeli. Kural "kaç kopya" değil, "kopyalar NEREDE".
 * </pre>
 */
class Ders9b_AnyOneTrapIT {

    private static Agents agents;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        agents = PgLabSupport.startSteadyCluster("ANY1", Lab.ENDPOINTS, Lab.ENDPOINTS);
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
    void acknowledgedTransferIsLostWhenTheOnlyAckCameFromTheSameBuilding() throws Exception {
        Lab.say("ssn = %s, /sync = %s", Lab.query("pg-ist", "SELECT current_setting('synchronous_standby_names')"),
                agents.bean("pg-ist", io.paramix.raftlab.etcd.EtcdStore.class).sync());

        Lab.say("① Ankara'nın iki standby'ı donuyor (ağ yavaş). Transfer yine de 'tamam' alıyor: onay pg-ist2'den");
        Lab.pause("pg-ank");
        Lab.pause("pg-ank2");
        // Dondurmak bağlantıyı kesmez: primary göndermeye devam eder, veri donmuş sürecin TCP tamponunda bekler ve
        // çözülünce okunur (Redis Lab'deki "eski master" deneyinin aynısı). Gerçek hat kopukluğu için bağlantıyı da kes.
        Lab.exec("pg-ist", "postgres", "SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name IN ('pg-ank', 'pg-ank2')");
        long t = System.currentTimeMillis();
        TransferResult r = LedgerService.transferOn("127.0.0.1", 5511, "pg-ist", "ali", "ayse", 99_900);
        Lab.say("   transfer %d 'tamam' aldı (%d ms). Ali'nin ekranında '✓ 999 TL gönderildi'", r.transferId(), System.currentTimeMillis() - t);

        Lab.say("② İstanbul yanıyor: pg-ist, pg-ist2, etcd-ist, iki ajan");
        agents.stop("pg-ist");
        agents.stop("pg-ist2");
        Lab.kill("pg-ist");
        Lab.kill("pg-ist2");
        Lab.kill("etcd-ist");
        Lab.resume("pg-ank");
        Lab.resume("pg-ank2");

        Lab.say("③ Ankara toparlanıyor: /sync'te adı var → lider olabilir");
        Awaitility.await().atMost(Duration.ofSeconds(40)).until(() ->
                agents.agent("pg-ank").role() == Role.PRIMARY || agents.agent("pg-ank2").role() == Role.PRIMARY);
        String leader = agents.agent("pg-ank").role() == Role.PRIMARY ? "pg-ank" : "pg-ank2";

        long found = ((Number) Lab.query(leader, "SELECT count(*) FROM transfers WHERE id = " + r.transferId())).longValue();
        Lab.say("④ Yeni lider %s. 'tamam' denen transfer %d orada mı? %s", leader, r.transferId(), found == 1 ? "EVET" : "HAYIR, KAYBOLDU");
        assertThat(found).as("ANY 1 ile onay aynı binadan geldi: transfer kayboldu").isZero();
        System.out.println(Lab.run("status"));
    }
}
