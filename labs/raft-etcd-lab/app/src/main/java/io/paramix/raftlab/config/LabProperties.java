package io.paramix.raftlab.config;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param node          bu ajanın yönettiği PostgreSQL'in adı (pg-ist, pg-ank, pg-ist2). Standby'ın application_name'i de bu
 * @param region        istanbul / ankara: senkron standby seçerken "başka bölgeden" tercih edilir
 * @param etcdEndpoints etcd düğümleri. Patroni'de hepsi verilir; split-brain deneyinde ajan sadece kendi bölgesininkini görür
 * @param scope         etcd'deki anahtarların kökü (/paramix/ledger/leader, /sync, /members/...)
 * @param ttl           lider kaydının ömrü. Lider bu süre içinde yenilemezse kayıt silinir (Patroni varsayılanı 30 sn)
 * @param loopWait      ajan döngüsü aralığı (Patroni: loop_wait 10 sn)
 * @param syncMode      SINGLE: tek senkron standby (başka bölgeden). ANY1: "ANY 1 (bütün standby'lar)", tuzağı göstermek için
 * @param strict        senkron standby yoksa commit'ler beklesin mi (RPO 0) yoksa asenkrona mı düşülsün
 */
@ConfigurationProperties("lab")
public record LabProperties(String node, String region, @DefaultValue("127.0.0.1") String pgHost, int pgPort,
                            @DefaultValue("postgres") String pgUser, @DefaultValue("ledger") String pgDatabase,
                            List<String> etcdEndpoints, @DefaultValue("/paramix/ledger") String scope,
                            @DefaultValue("6s") Duration ttl, @DefaultValue("1s") Duration loopWait,
                            @DefaultValue("1s") Duration etcdTimeout, @DefaultValue("SINGLE") SyncMode syncMode,
                            @DefaultValue("true") boolean strict) {

    public enum SyncMode { SINGLE, ANY1 }

    public long ttlSeconds() {
        return Math.max(2, ttl.toSeconds());
    }
}
