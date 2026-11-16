package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import io.paramix.raftlab.EtcdProbe.Member;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 3–5: lider ölünce ve çoğunluk kaybolunca.
 *
 * <pre>
 *  ① Raft liderini öldür → kalan iki düğüm ~1 sn'de yeni lider seçer, DÖNEM (term) artar
 *  ② Kesinleşmiş (commit edilmiş) hiçbir yazma kaybolmaz; 2/3 ile yazmaya devam
 *  ③ Bir düğüm daha gider (1/3): YAZMA DURUR. Doğrusal okuma da durur; "serializable" okuma
 *     düğümün kendi (belki eski) kopyasından cevap verir
 *  ④ Düğümler dönünce küme toparlanır, commit edilmiş hiçbir şey kaybolmamıştır.
 *     Zaman aşımı alan yazma ise ya kesinleşmiş ya silinmiş olabilir: "sonuç bilinmiyor"
 * </pre>
 */
class Ders3_5_RaftElectionIT {

    private static EtcdProbe etcd;

    @BeforeAll
    static void up() {
        assumeTrue(Lab.available(), "etcd / PostgreSQL ikili dosyaları yok");
        Lab.fresh();
        etcd = new EtcdProbe(Lab.ENDPOINTS);
    }

    @AfterAll
    static void down() {
        if (etcd != null) {
            etcd.close();
            Lab.down();
        }
    }

    @Test
    void leaderDiesElectionMinorityStopsWritesRecoveryLosesNothing() throws Exception {
        for (int i = 1; i <= 5; i++) {
            etcd.put("/ders/transfer/" + i, "kesin-" + i, 2_000);           // hepsi commit edildi ("tamam" alındı)
        }
        Map<String, Member> before = etcd.statusAll();
        etcd.print("başlangıç");
        Member oldLeader = before.values().stream().filter(Member::leader).findFirst().orElseThrow();

        Lab.say("① Raft lideri %s öldürülüyor (term %d)", oldLeader.name(), oldLeader.term());
        long killedAt = System.currentTimeMillis();
        Lab.kill(oldLeader.name());
        AtomicReference<Member> newLeader = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).until(() -> {
            newLeader.set(etcd.statusAll().values().stream().filter(m -> m != null && m.leader()).findFirst().orElse(null));
            return newLeader.get() != null;
        });
        long electionMs = System.currentTimeMillis() - killedAt;
        etcd.print("seçimden sonra (" + electionMs + " ms)");
        Lab.say("Yeni lider %s, term %d → %d. Seçim %d ms sürdü (election-timeout 1000 ms)",
                newLeader.get().name(), oldLeader.term(), newLeader.get().term(), electionMs);
        assertThat(newLeader.get().term()).as("yeni seçim = yeni dönem").isGreaterThan(oldLeader.term());
        assertThat(newLeader.get().name()).isNotEqualTo(oldLeader.name());

        Lab.say("② Commit edilmiş 5 yazmanın hepsi yeni liderde var; 2/3 ile yazmaya devam");
        String alive = EtcdProbe.endpointsExcept(oldLeader.name());
        for (int i = 1; i <= 5; i++) {
            assertThat(EtcdProbe.get(newLeader.get().endpoint(), "/ders/transfer/" + i, false, 2_000)).isEqualTo("kesin-" + i);
        }
        EtcdProbe.putVia(alive, "/ders/transfer/6", "kesin-6", 3_000);

        Member second = etcd.statusAll().values().stream().filter(m -> m != null && !m.leader()).findFirst().orElseThrow();
        Member last = newLeader.get();
        Lab.say("③ %s de öldürülüyor: geriye tek düğüm (%s) kaldı = 1/3, çoğunluk YOK", second.name(), last.name());
        Lab.kill(second.name());
        assertThatThrownBy(() -> EtcdProbe.putVia(last.endpoint(), "/ders/transfer/7", "olmamalı", 3_000))
                .as("yazma: çoğunluk onayı alınamaz").isInstanceOf(java.util.concurrent.TimeoutException.class);
        assertThatThrownBy(() -> EtcdProbe.get(last.endpoint(), "/ders/transfer/6", false, 3_000))
                .as("doğrusal okuma: 'hâlâ lider miyim?' teyidi çoğunluk ister").isInstanceOf(Exception.class);
        assertThat(EtcdProbe.get(last.endpoint(), "/ders/transfer/6", true, 2_000))
                .as("serializable okuma: düğüm kendi kopyasından cevap verir (eski olabilir!)").isEqualTo("kesin-6");
        Lab.say("   yazma: ZAMAN AŞIMI · doğrusal okuma: HATA · serializable okuma: '%s' (kendi kopyası)",
                EtcdProbe.get(last.endpoint(), "/ders/transfer/6", true, 2_000));

        Lab.say("④ İki düğüm geri geliyor");
        Lab.start(oldLeader.name());
        Lab.start(second.name());
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
            Map<String, Member> s = etcd.statusAll();
            return s.values().stream().allMatch(Objects::nonNull) && s.values().stream().filter(Member::leader).count() == 1
                    && s.values().stream().map(Member::raftIndex).distinct().count() == 1;
        });
        etcd.print("toparlandıktan sonra");
        Lab.say("Term neden bu kadar arttı? Tek kalan düğüm çoğunluk bulamadığı için her election-timeout'ta yeni bir"
                + " seçim başlattı (her seferinde term+1) ve hepsini kaybetti. Dönen düğümler en büyük term'i benimser.");
        for (int i = 1; i <= 6; i++) {
            assertThat(EtcdProbe.get(Lab.ENDPOINTS, "/ders/transfer/" + i, false, 2_000)).isEqualTo("kesin-" + i);
        }
        // Zaman aşımı alan yazma: tek kalan düğüm onu log'una EKLEMİŞTİ ama commit edememişti. Diğerleri dönünce:
        //   - o düğüm yeniden lider seçilirse satırı kopyalar → yazma SONRADAN kesinleşir
        //   - log'unda o satır olmayan biri lider olursa satır SİLİNİR
        // İkisi de Raft'a göre doğru. İstemci açısından: zaman aşımı = "sonuç BİLİNMİYOR", "başarısız" değil.
        String timedOut = EtcdProbe.get(Lab.ENDPOINTS, "/ders/transfer/7", false, 2_000);
        Lab.say("Zaman aşımı alan yazma (/ders/transfer/7) şimdi: %s", timedOut == null
                ? "YOK. Log'unda o satır olmayan bir düğüm lider oldu, kesinleşmemiş satır silindi"
                : "VAR! Satırı log'unda tutan düğüm yeniden lider oldu ve onu kesinleştirdi");
        Lab.say("Ders: zaman aşımı 'başarısız' değil, 'sonuç bilinmiyor' demek. Tekrar denemek güvenli olsun diye idempotency anahtarı şart");
        assertThat(timedOut).isIn(null, "olmamalı");
    }
}
