package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import io.paramix.raftlab.EtcdProbe.Member;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ders 1–2: "Üç makinede aynı sıralı listeyi nasıl tutarız?"
 *
 * <pre>
 *  ① Bir lider var, herkes aynı dönemde (term)
 *  ② Yazmalar log'a eklenir: üç düğümün log'u (raftIndex) aynı numaraya gelir
 *  ③ Bir takipçi donmuşken yazma YİNE BAŞARILI (lider + 1 takipçi = 2/3 çoğunluk)
 *  ④ Donmuş takipçi geri gelince eksiklerini alır ve yetişir
 * </pre>
 */
class Ders1_2_RaftLogIT {

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
    void theSameOrderedLogOnThreeMachines() throws Exception {
        Lab.say("① Küme açıldı: bir lider, herkes aynı dönemde");
        Map<String, Member> s = etcd.statusAll();
        etcd.print("başlangıç");
        assertThat(s.values()).allMatch(Objects::nonNull);
        assertThat(s.values().stream().filter(Member::leader)).hasSize(1);
        assertThat(s.values().stream().map(Member::term).distinct()).hasSize(1);

        Lab.say("② 10 yazma: her biri log'a bir satır. Üç düğümün raftIndex'i aynı sayıya gelmeli");
        for (int i = 1; i <= 10; i++) {
            etcd.put("/ders/hamle/" + i, "hamle-" + i, 2_000);
        }
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                etcd.statusAll().values().stream().map(Member::raftIndex).distinct().count() == 1);
        etcd.print("10 yazmadan sonra");
        long indexAfter10 = etcd.statusAll().get("etcd-ist").raftIndex();

        Lab.say("③ Bir takipçiyi donduruyoruz. Yazmalar yine başarılı olmalı: lider + diğer takipçi = 2/3");
        String follower = s.values().stream().filter(m -> !m.leader()).findFirst().orElseThrow().name();
        Lab.pause(follower);
        try {
            for (int i = 11; i <= 15; i++) {
                // (istemci donmuş düğüme bağlıysa asılı kalır; gerçek istemci zaman aşımında diğer uca geçer)
                EtcdProbe.putVia(EtcdProbe.endpointsExcept(follower), "/ders/hamle/" + i, "hamle-" + i, 3_000);   // exception yok = commit edildi
            }
            Map<String, Member> during = etcd.statusAll();
            etcd.print(follower + " donmuşken 5 yazma daha");
            assertThat(during.get(follower)).as("donmuş düğüm cevap vermez").isNull();
            assertThat(during.values().stream().filter(Objects::nonNull).map(Member::raftIndex))
                    .allMatch(i -> i >= indexAfter10 + 5);
        } finally {
            Lab.say("④ " + follower + " çözüldü: lider ona eksik satırları gönderir, yetişir");
            Lab.resume(follower);
        }
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map<String, Member> now = etcd.statusAll();
            return now.values().stream().allMatch(Objects::nonNull)
                    && now.values().stream().map(Member::raftIndex).distinct().count() == 1;
        });
        etcd.print("yetiştikten sonra");
        assertThat(EtcdProbe.get(Lab.ENDPOINTS.split(",")[0], "/ders/hamle/15", true, 2_000)).isEqualTo("hamle-15");
    }
}
