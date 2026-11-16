package io.paramix.raftlab;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.GetOption;

/** Testlerin etcd'ye doğrudan bakması için: her düğümün Raft durumu, okuma/yazma. */
public final class EtcdProbe implements AutoCloseable {

    /** Bir düğümün Raft'taki durumu: lider mi, hangi dönem (term), log'unda kaçıncı satıra kadar var (index). */
    public record Member(String name, String endpoint, long id, boolean leader, long term, long raftIndex) {
        @Override
        public String toString() {
            return String.format("%-9s %-6s term=%-3d raftIndex=%d", name, leader ? "LİDER" : "takipçi", term, raftIndex);
        }
    }

    public static final Map<String, String> NAMES = Map.of(
            Lab.ETCD_IST, "etcd-ist", Lab.ETCD_ANK, "etcd-ank", Lab.ETCD_WIT, "etcd-wit");

    private final Client client;

    public EtcdProbe(String endpoints) {
        this.client = Client.builder().endpoints(endpoints.split(",")).build();
    }

    public Client client() {
        return client;
    }

    /** null = düğüme ulaşılamadı (ölü ya da donmuş). */
    public Member status(String endpoint) {
        try (Client one = Client.builder().endpoints(endpoint).build()) {
            var r = one.getMaintenanceClient().statusMember(endpoint).get(1500, TimeUnit.MILLISECONDS);
            long me = r.getHeader().getMemberId();
            return new Member(NAMES.get(endpoint), endpoint, me, r.getLeader() == me, r.getRaftTerm(), r.getRaftIndex());
        } catch (Exception e) {
            return null;
        }
    }

    public Map<String, Member> statusAll() {
        Map<String, Member> out = new LinkedHashMap<>();
        for (String ep : List.of(Lab.ETCD_IST, Lab.ETCD_ANK, Lab.ETCD_WIT)) {
            out.put(NAMES.get(ep), status(ep));
        }
        return out;
    }

    public void print(String title) {
        System.out.println("   ┌ " + title);
        statusAll().forEach((n, m) -> System.out.println("   │ " + (m == null ? String.format("%-9s ulaşılamıyor", n) : m)));
        System.out.println("   └");
    }

    public void put(String key, String value, long timeoutMs) throws Exception {
        client.getKVClient().put(bs(key), bs(value)).get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Sadece verilen uç noktalar üzerinden yaz (donmuş düğüme bağlı kalıp asılmamak için). */
    public static void putVia(String endpoints, String key, String value, long timeoutMs) throws Exception {
        try (Client one = Client.builder().endpoints(endpoints.split(",")).build()) {
            one.getKVClient().put(bs(key), bs(value)).get(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    public static String endpointsExcept(String name) {
        return String.join(",", NAMES.entrySet().stream().filter(e -> !e.getValue().equals(name)).map(Map.Entry::getKey).sorted().toList());
    }

    /** serializable=false: doğrusal okuma (Raft liderine sorulur, çoğunluk gerekir). true: düğüm kendi kopyasından cevap verir. */
    public static String get(String endpoints, String key, boolean serializable, long timeoutMs) throws Exception {
        try (Client one = Client.builder().endpoints(endpoints.split(",")).build()) {
            var kvs = one.getKVClient().get(bs(key), GetOption.builder().withSerializable(serializable).build())
                    .get(timeoutMs, TimeUnit.MILLISECONDS).getKvs();
            return kvs.isEmpty() ? null : kvs.get(0).getValue().toString(StandardCharsets.UTF_8);
        }
    }

    public static ByteSequence bs(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        client.close();
    }
}
