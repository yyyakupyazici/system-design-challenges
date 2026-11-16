package io.paramix.raftlab.etcd;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import io.paramix.raftlab.config.LabProperties;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

/**
 * etcd'deki "ilan panosu". Üç anahtar:
 *
 * <pre>
 *  {scope}/leader            = pg-ist           lease'e bağlı: sahibi yenilemezse ttl sonunda KENDİLİĞİNDEN silinir
 *  {scope}/sync              = pg-ank           "tamam" denen her commit'i kesin olarak alan standby(lar).
 *                                               Failover'da SADECE bunlar lider olabilir
 *  {scope}/members/{node}    = {json durum}     her ajan kendi PostgreSQL'inin durumunu yayınlar (lease'li)
 * </pre>
 *
 * Bütün yazmalar Raft'tan geçer: çoğunluk (3'te 2) diskine yazmadan "tamam" dönmez. Çoğunluk yoksa çağrılar zaman
 * aşımına uğrar ve ajan bunu "panoya ulaşamıyorum" olarak yorumlar.
 */
@Component
public class EtcdStore {

    public record LeaderRecord(String node, long createRevision, long modRevision, long lease) {
    }

    private final Client client;
    private final LabProperties props;
    private final ByteSequence leaderKey;
    private final ByteSequence syncKey;
    private final String membersPrefix;

    public EtcdStore(LabProperties props) {
        this.props = props;
        this.client = Client.builder().endpoints(props.etcdEndpoints().toArray(String[]::new)).build();
        this.leaderKey = bs(props.scope() + "/leader");
        this.syncKey = bs(props.scope() + "/sync");
        this.membersPrefix = props.scope() + "/members/";
    }

    @PreDestroy
    void close() {
        client.close();
    }

    public Client client() {
        return client;
    }

    // ------------------------------------------------------------------ lease

    /** Süreli kira. ttl saniye boyunca yenilenmezse, ona bağlı bütün anahtarlar silinir. */
    public long grantLease() {
        return await(client.getLeaseClient().grant(props.ttlSeconds())).getID();
    }

    /**
     * Kirayı bir kez uzatır. Dönüş: true = uzatıldı, false = kira artık YOK (süresi dolmuş, anahtarları silinmiş).
     * Çoğunluğa ulaşılamıyorsa exception (ne olduğunu bilmiyoruz → çağıran temkinli davranmalı).
     */
    public boolean keepAlive(long leaseId) {
        try {
            return client.getLeaseClient().keepAliveOnce(leaseId).get(props.etcdTimeout().toMillis(), TimeUnit.MILLISECONDS).getTTL() > 0;
        } catch (ExecutionException e) {
            if (String.valueOf(e.getCause()).contains("lease not found")) {
                return false;
            }
            throw new EtcdUnavailableException(e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            throw new EtcdUnavailableException(e);
        }
    }

    // ------------------------------------------------------------------ lider

    /** Doğrusal (linearizable) okuma: Raft liderine sorar, çoğunluk olmadan cevap vermez. Eski bir lideri asla göstermez. */
    public Optional<LeaderRecord> leader() {
        var kvs = await(client.getKVClient().get(leaderKey)).getKvs();
        return kvs.isEmpty() ? Optional.empty() : Optional.of(toLeader(kvs.get(0)));
    }

    /**
     * "Lider kaydı YOKSA benim adımı yaz", tek atomik adım (compare-and-set). İki aday aynı anda denese de Raft
     * işlemleri tek sıraya dizdiği için sadece biri kazanır. createRevision == 0 → "bu anahtar hiç yok" demek.
     */
    public boolean tryAcquireLeader(String node, long leaseId) {
        return await(client.getKVClient().txn()
                .If(new Cmp(leaderKey, Cmp.Op.EQUAL, CmpTarget.createRevision(0)))
                .Then(Op.put(leaderKey, bs(node), PutOption.builder().withLeaseId(leaseId).build()))
                .commit()).isSucceeded();
    }

    /** Liderliği bilerek bırak (PostgreSQL çöktüyse ve yerine kimse geçemiyorsa TTL'i beklemeden). Sadece sahibiysem. */
    public boolean releaseLeader(String node) {
        return await(client.getKVClient().txn()
                .If(new Cmp(leaderKey, Cmp.Op.EQUAL, CmpTarget.value(bs(node))))
                .Then(Op.delete(leaderKey, io.etcd.jetcd.options.DeleteOption.DEFAULT))
                .commit()).isSucceeded();
    }

    /** Lider kaydı değişince haber ver (sürekli sormak yerine). Silinme = lider gitti, seçim zamanı. */
    public Watch.Watcher watchLeader(Consumer<WatchEvent.EventType> onEvent) {
        return client.getWatchClient().watch(leaderKey, Watch.listener(r -> r.getEvents().forEach(e -> onEvent.accept(e.getEventType()))));
    }

    // ------------------------------------------------------------------ senkron standby

    public Set<String> sync() {
        var kvs = await(client.getKVClient().get(syncKey)).getKvs();
        Set<String> out = new TreeSet<>();
        if (!kvs.isEmpty()) {
            for (String s : str(kvs.get(0).getValue()).split(",")) {
                if (!s.isBlank()) {
                    out.add(s.trim());
                }
            }
        }
        return out;
    }

    /** /sync'i SADECE hâlâ liderken yaz: liderliği kaybetmiş eski primary bu kaydı bozamasın (değer karşılaştırmalı txn). */
    public boolean putSyncIfLeader(String me, Set<String> names) {
        return await(client.getKVClient().txn()
                .If(new Cmp(leaderKey, Cmp.Op.EQUAL, CmpTarget.value(bs(me))))
                .Then(Op.put(syncKey, bs(String.join(",", names)), PutOption.DEFAULT))
                .commit()).isSucceeded();
    }

    // ------------------------------------------------------------------ üyeler

    public void putMember(String node, String json, long leaseId) {
        await(client.getKVClient().put(bs(membersPrefix + node), bs(json), PutOption.builder().withLeaseId(leaseId).build()));
    }

    public Map<String, String> members() {
        var kvs = await(client.getKVClient().get(bs(membersPrefix), GetOption.builder().isPrefix(true).build())).getKvs();
        Map<String, String> out = new LinkedHashMap<>();
        kvs.forEach(kv -> out.put(str(kv.getKey()).substring(membersPrefix.length()), str(kv.getValue())));
        return out;
    }

    // ------------------------------------------------------------------ yardımcılar

    private static LeaderRecord toLeader(KeyValue kv) {
        return new LeaderRecord(str(kv.getValue()), kv.getCreateRevision(), kv.getModRevision(), kv.getLease());
    }

    private <T> T await(CompletableFuture<T> f) {
        try {
            return f.get(props.etcdTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new EtcdUnavailableException(e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            f.cancel(true);
            throw new EtcdUnavailableException(e);
        }
    }

    public static ByteSequence bs(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    public static String str(ByteSequence b) {
        return b.toString(StandardCharsets.UTF_8);
    }

    /** etcd çoğunluğuna ulaşılamadı ya da zaman aşımı: "pano şu an bana cevap vermiyor". */
    public static class EtcdUnavailableException extends RuntimeException {
        public EtcdUnavailableException(Throwable cause) {
            super(String.valueOf(cause), cause);
        }
    }
}
