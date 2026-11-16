package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;

import io.paramix.raftlab.ledger.LedgerService;
import io.paramix.raftlab.ledger.LedgerService.BalanceRead;
import io.paramix.raftlab.ledger.LedgerService.TransferResult;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 10: yazma primary'ye, gösterilen okuma standby'a. "Az önce gönderdim, ekranda yok!"
 *
 * <pre>
 *  pg-ist2'ye yapay 2 sn gecikme (recovery_min_apply_delay): yavaş/uzak bir replika
 *  ① Transfer → commit LSN'i (ör. 0/5012A30)
 *  ② pg-ist2'den düz okuma → ESKİ bakiye (para kaybı değil, görüntü sorunu)
 *  ③ "En az 0/5012A30'u uygulamış ol" şartıyla pg-ist2: 1 sn içinde yetişemez → primary'den okur
 *  ④ Aynı şartla pg-ank (senkron standby): WAL zaten diskinde, uygulaması ms'ler sürer → kendisi cevap verir
 * </pre>
 */
class Ders10_ReadYourWritesIT {

    private static Agents agents;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        agents = PgLabSupport.startSteadyCluster("SINGLE", Lab.ENDPOINTS, Lab.ENDPOINTS);
    }

    @AfterAll
    static void down() {
        if (agents != null) {
            agents.close();
            Lab.down();
        }
    }

    @Test
    void readingFromAStandbyCanBeStaleUnlessYouWaitForTheCommitLsn() throws Exception {
        Lab.exec("pg-ist2", "postgres", "ALTER SYSTEM SET recovery_min_apply_delay = '2s'");
        Lab.exec("pg-ist2", "postgres", "SELECT pg_reload_conf()");
        LedgerService ledger = agents.bean("pg-ank", LedgerService.class);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ledger.balance("ayse", "pg-ist2", null, 0).balance() == 0);

        TransferResult r = ledger.transfer("ali", "ayse", 25_000);
        Lab.say("① Transfer 250 TL → primary %s, commit LSN %s", r.primary(), r.commitLsn());

        BalanceRead stale = ledger.balance("ayse", "pg-ist2", null, 0);
        Lab.say("② pg-ist2'den düz okuma: Ayşe'nin bakiyesi %d (replay %s) → ESKİ. Para kaybı yok, ekran geride",
                stale.balance(), stale.replayLsn());
        assertThat(stale.balance()).isZero();

        BalanceRead guarded = ledger.balance("ayse", "pg-ist2", r.commitLsn(), 1_000);
        Lab.say("③ LSN şartıyla pg-ist2: %d ms bekledi, yetişemedi → %s'den okundu: %d", guarded.waitedMs(),
                guarded.servedBy(), guarded.balance());
        assertThat(guarded.fellBackToPrimary()).isTrue();
        assertThat(guarded.balance()).isEqualTo(25_000);

        BalanceRead sync = ledger.balance("ayse", "pg-ank", r.commitLsn(), 1_000);
        Lab.say("④ LSN şartıyla pg-ank (senkron standby): %d ms bekledi, kendisi cevap verdi: %d (replay %s)",
                sync.waitedMs(), sync.balance(), sync.replayLsn());
        assertThat(sync.fellBackToPrimary()).isFalse();
        assertThat(sync.balance()).isEqualTo(25_000);

        Lab.say("Karar veren okuma ('bakiye yeterli mi?') ise HİÇ standby'a gitmez: transfer içinde, primary'de koşullu UPDATE");
    }
}
