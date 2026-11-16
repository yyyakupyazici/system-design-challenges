package io.paramix.raftlab.ha;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import io.paramix.raftlab.config.LabProperties;
import io.paramix.raftlab.config.LabProperties.SyncMode;
import io.paramix.raftlab.etcd.EtcdStore;
import io.paramix.raftlab.etcd.EtcdStore.EtcdUnavailableException;
import io.paramix.raftlab.etcd.EtcdStore.LeaderRecord;
import io.paramix.raftlab.pg.PgNode;
import io.paramix.raftlab.pg.PgNode.Status;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Mini-Patroni: bir PostgreSQL'in yanındaki "bekçi". Her döngüde (loop-wait) tek bir soru sorar:
 * <b>"Panoda lider olarak kim yazıyor, ve ben buna göre ne yapmalıyım?"</b>
 *
 * <pre>
 *  lider BENİM        → kirayı uzat. Uzatamazsam ve son başarılı uzatma ttl/2'den eskiyse: KENDİMİ KAPAT (fence)
 *                       yazma kapalıysa aç, senkron standby'ı yönet
 *  lider BAŞKASI      → standby'sam: onu takip et.  Primary'sem: ben eski primary'yim → kendimi kapat
 *  lider YOK          → uygun muyum? (standby'sam sadece /sync'te adım varsa) → "yoksa yaz" txn → kazanırsam promote
 *  pano CEVAP VERMİYOR → liderdiysem ve ttl/2 geçtiyse kendimi kapat (ötekiler ttl sonunda lider seçecek)
 * </pre>
 *
 * Split-brain'i önleyen zamanlama: eski lider en geç ttl/2'de kendini kapatır, yeni lider en erken ttl'de seçilebilir
 * (kira ancak o zaman düşer). Arada kimsenin yazmadığı bir boşluk kalır; iki primary'nin aynı anda yazdığı an olmaz.
 */
@Component
public class HaAgent {

    private static final Logger log = LoggerFactory.getLogger(HaAgent.class);

    public enum Role { STARTING, PRIMARY, STANDBY, FENCED, PG_DOWN, NO_QUORUM }

    public record Event(Instant at, String node, Role role, String message) {
        @Override
        public String toString() {
            return at + " [" + node + "] " + role + " " + message;
        }
    }

    public record Member(String node, String region, String host, int port, String role, long lsnBytes, String lsn) {
    }

    private final LabProperties props;
    private final EtcdStore etcd;
    private final PgNode pg;
    private final ObjectMapper json;
    private final String me;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Event> events = new CopyOnWriteArrayList<>();

    private volatile Role role = Role.STARTING;
    private volatile long leaseId;
    private volatile long lastRenewOk;
    private volatile String lastLeaderSeen;
    private volatile String lastMessage = "";
    private volatile boolean stopped;
    private Watch.Watcher watcher;

    public HaAgent(LabProperties props, EtcdStore etcd, PgNode pg, ObjectMapper json) {
        this.props = props;
        this.etcd = etcd;
        this.pg = pg;
        this.json = json;
        this.me = props.node();
    }

    @PostConstruct
    void start() {
        // Watch: lider kaydı silinince (kira doldu ya da bırakıldı) bir sonraki döngüyü beklemeden hemen bak
        watcher = etcd.watchLeader(type -> {
            if (type == WatchEvent.EventType.DELETE && !stopped) {
                Thread.ofVirtual().start(this::tick);
            }
        });
    }

    @PreDestroy
    void stop() {
        stopped = true;
        if (watcher != null) {
            watcher.close();
        }
    }

    public Role role() {
        return role;
    }

    public List<Event> events() {
        return List.copyOf(events);
    }

    // ------------------------------------------------------------------ döngü

    @Scheduled(fixedDelayString = "${lab.loop-wait:1s}")
    public void tick() {
        if (stopped || !lock.tryLock()) {
            return;
        }
        try {
            doTick();
        } finally {
            lock.unlock();
        }
    }

    private void doTick() {
        Status st = pg.statusOrNull();
        if (st == null) {
            onPgDown();
            return;
        }
        try {
            ensureLease();
            etcd.putMember(me, memberJson(st), leaseId);
            Optional<LeaderRecord> leader = etcd.leader();
            if (leader.isPresent() && leader.get().node().equals(me)) {
                onLeader(st, leader.get());
            } else if (leader.isPresent()) {
                lastLeaderSeen = leader.get().node();
                onFollower(st, leader.get());
            } else {
                onNoLeader(st);
            }
        } catch (EtcdUnavailableException e) {
            onEtcdUnavailable(e);
        } catch (SQLException e) {
            note(role, "PostgreSQL komutu başarısız: " + e.getMessage());
        }
    }

    /** Oturum kirası: lider kaydı ve üye kaydı buna bağlı. Kira düşerse ikisi de etcd'den silinir. */
    private void ensureLease() {
        if (leaseId != 0 && etcd.keepAlive(leaseId)) {
            return;
        }
        if (leaseId != 0) {
            note(role, "kiram (lease " + Long.toHexString(leaseId) + ") düşmüş: ona bağlı lider kaydım da silinmiştir");
        }
        leaseId = etcd.grantLease();
    }

    // ------------------------------------------------------------------ lider benim

    private void onLeader(Status st, LeaderRecord leader) throws SQLException {
        lastRenewOk = System.currentTimeMillis();       // ensureLease az önce kirayı uzattı ve kayıt hâlâ benim
        if (st.inRecovery()) {
            note(Role.PRIMARY, "lider kaydı bende ama PostgreSQL standby: promote ediyorum");
            pg.promote();
        }
        if (st.readOnly()) {
            pg.unfence();
            note(Role.PRIMARY, "yazmaya açtım");
        }
        set(Role.PRIMARY, "liderim (lider kaydı revision " + leader.createRevision() + "), kiramı uzattım");
        manageSync(pg.statusOrNull());
    }

    // ------------------------------------------------------------------ lider başkası

    private void onFollower(Status st, LeaderRecord leader) throws SQLException {
        if (!st.inRecovery()) {
            // Ben primary'yim ama panoda lider başkası: kopukken yerime biri seçilmiş. Asla yazmaya devam etme.
            if (!st.readOnly()) {
                pg.fence();
            }
            set(Role.FENCED, "lider " + leader.node() + ", ben eski primary'yim → yazmaya kapalıyım"
                    + " (geri katılmak için pg_rewind + standby olarak başlatma gerekir)");
            return;
        }
        Member m = member(leader.node());
        if (m != null && !st.primaryConninfo().contains("port=" + m.port())) {
            pg.follow(m.host(), m.port());
            note(Role.STANDBY, "yeni primary " + leader.node() + ":" + m.port() + " → onu takip ediyorum");
        }
        set(Role.STANDBY, "lider " + leader.node() + ", ben standby'ım");
    }

    // ------------------------------------------------------------------ lider yok

    private void onNoLeader(Status st) throws SQLException {
        Set<String> sync = etcd.sync();
        String why;
        if (!st.inRecovery()) {
            if (st.readOnly() && lastLeaderSeen != null && !lastLeaderSeen.equals(me)) {
                set(Role.FENCED, "lider yok ama ben devrilmiş eski primary'yim: aday olmuyorum");
                return;
            }
            why = "primary'yim ve lider kaydı boş (ilk açılış ya da kiram düştü)";
        } else if (props.strict()) {
            if (!sync.contains(me)) {
                set(Role.STANDBY, "lider yok ama aday olamam: /sync = " + sync + " (\"tamam\" denen her şeyin bende olduğu garanti değil)");
                return;
            }
            Member best = bestOf(sync);
            if (best != null && best.lsnBytes() > st.lsnBytes()) {
                set(Role.STANDBY, "lider yok, /sync'te adım var ama " + best.node() + " benden ileride: ona bırakıyorum");
                return;
            }
            why = "/sync = " + sync + " içindeyim: \"tamam\" denen her commit bende";
        } else {
            Member best = bestOf(etcd.members().keySet());
            if (best != null && !best.node().equals(me) && best.lsnBytes() > st.lsnBytes()) {
                set(Role.STANDBY, "lider yok, en ileride olan " + best.node() + ": ona bırakıyorum");
                return;
            }
            why = "strict değil, en ilerideki standby benim";
        }

        note(role, "lider yok. Aday oluyorum: " + why);
        if (!etcd.tryAcquireLeader(me, leaseId)) {
            note(role, "\"yoksa yaz\" txn'i başarısız: başka biri benden önce lider oldu");
            return;
        }
        lastRenewOk = System.currentTimeMillis();
        lastLeaderSeen = me;
        note(Role.PRIMARY, "lider kaydını ALDIM (compare-and-set başarılı)");
        if (st.inRecovery()) {
            pg.promote();
            note(Role.PRIMARY, "PostgreSQL promote edildi: artık yazma kabul ediyorum");
        }
        Status now = pg.statusOrNull();
        if (now != null && now.readOnly()) {
            pg.unfence();
        }
        set(Role.PRIMARY, "liderim");
        manageSync(pg.statusOrNull());
    }

    // ------------------------------------------------------------------ pano cevap vermiyor

    private void onEtcdUnavailable(EtcdUnavailableException e) {
        long since = System.currentTimeMillis() - lastRenewOk;
        if (role == Role.PRIMARY) {
            if (since > props.ttl().toMillis() / 2) {
                try {
                    pg.fence();
                } catch (SQLException ex) {
                    note(role, "fence başarısız: " + ex.getMessage());
                }
                set(Role.FENCED, "etcd çoğunluğuna " + since + " ms'dir ulaşamıyorum, kiram her an düşebilir"
                        + " → KENDİMİ YAZMAYA KAPATTIM (ötekiler ttl sonunda lider seçebilir)");
            } else {
                note(role, "etcd'ye ulaşamıyorum (" + since + " ms), kiram hâlâ geçerli olabilir, bekliyorum");
            }
        } else if (role != Role.FENCED) {
            set(Role.NO_QUORUM, "etcd çoğunluğuna ulaşamıyorum: hiçbir karar vermiyorum");
        }
    }

    private void onPgDown() {
        set(Role.PG_DOWN, "PostgreSQL'e ulaşamıyorum");
        try {
            if (etcd.releaseLeader(me)) {
                note(Role.PG_DOWN, "PostgreSQL'im çalışmıyor: lider kaydını BIRAKTIM (ttl'i beklemesinler)");
            }
        } catch (EtcdUnavailableException ignored) {
            // pano da yok: kira ttl sonunda kendiliğinden düşer
        }
    }

    // ------------------------------------------------------------------ senkron standby yönetimi (sadece lider)

    /**
     * SINGLE: tek senkron standby, tercihen başka bölgeden. Değiştirirken sıra önemli (Patroni'deki gibi):
     * <pre>
     *  ① /sync = {eski, yeni}     failover olursa ikisinden en ileride olan seçilir
     *  ② synchronous_standby_names = yeni
     *  ③ yeni gerçekten 'sync' olunca → /sync = {yeni}
     * </pre>
     * /sync bir SÖZ: "burada adı geçen, 'tamam' denen her commit'e sahip". Söz, PostgreSQL gerçekten öyle
     * çalışmaya başlamadan daraltılmaz.
     *
     * ANY1: "ANY 1 (bütün standby'lar)" ve /sync = hepsi. Bölgeyi umursamaz: tuzağı göstermek için.
     */
    private void manageSync(Status st) throws SQLException {
        if (st == null) {
            return;
        }
        Set<String> streaming = pg.streamingStandbys();          // ölü üyenin kaydı ttl boyunca durabilir: bağlı olana bak
        List<Member> standbys = new ArrayList<>(etcd.members().values().stream().map(this::parse)
                .filter(m -> m != null && !m.node().equals(me) && "STANDBY".equals(m.role()) && streaming.contains(m.node()))
                .toList());
        Set<String> current = new TreeSet<>(etcd.sync());
        current.remove(me);                                       // lider kendini /sync'te tutmaz

        if (props.syncMode() == SyncMode.ANY1) {
            // BİLEREK SAF: bağlı olsun olmasın bilinen bütün standby'lar, bölgesine bakmadan. "Kaç kopya" var ama
            // "kopyalar nerede" yok. (Patroni'nin quorum modu bunu bağlı olanlara göre günceller; tuzak buradaki gibi
            // elle yazılmış, statik bir ANY 1 ayarında ortaya çıkar.)
            Set<String> names = new TreeSet<>();
            etcd.members().values().stream().map(this::parse)
                    .filter(m -> m != null && !m.node().equals(me) && "STANDBY".equals(m.role()))
                    .forEach(m -> names.add(m.node()));
            if (names.isEmpty()) {
                return;
            }
            String ssn = "ANY 1 (" + String.join(", ", names.stream().map(HaAgent::q).toList()) + ")";
            if (!ssn.equals(st.syncNames())) {
                pg.setSyncNames(ssn);
                note(Role.PRIMARY, "synchronous_standby_names = " + ssn);
            }
            if (!current.equals(names)) {
                etcd.putSyncIfLeader(me, names);
            }
            return;
        }

        // En iyi aday: başka bölgeden olan önce (İstanbul komple yanarsa "tamam" denen veri Ankara'da olmalı), sonra en ileride olan
        Member best = standbys.stream()
                .sorted(Comparator.comparing((Member m) -> m.region().equals(props.region()))     // başka bölge önce
                        .thenComparing(Comparator.comparingLong(Member::lsnBytes).reversed())
                        .thenComparing(Member::node))
                .findFirst().orElse(null);
        // Mevcut senkron standby sağlıklıysa ve en iyi adayla aynı sınıftaysa (ikisi de başka bölgede) değiştirme: gereksiz geçiş yok
        String keep = best == null ? null : standbys.stream()
                .filter(m -> current.contains(m.node()) && m.region().equals(props.region()) == best.region().equals(props.region()))
                .map(Member::node).findFirst().orElse(null);
        String target = keep != null ? keep : (best == null ? null : best.node());
        if (target == null) {
            if (props.strict() && !current.isEmpty()) {
                note(Role.PRIMARY, "senkron standby yok (strict): commit'ler " + current + " dönene kadar BEKLER");
            }
            return;
        }
        if (!current.contains(target)) {
            Set<String> widened = new TreeSet<>(current);
            widened.add(target);
            etcd.putSyncIfLeader(me, widened);                                                // ①
        }
        if (!q(target).equals(st.syncNames())) {
            pg.setSyncNames(q(target));                                                       // ②
            note(Role.PRIMARY, "synchronous_standby_names = " + q(target));
        }
        if (!current.equals(Set.of(target)) && pg.isSyncStandby(target)) {
            etcd.putSyncIfLeader(me, Set.of(target));                                         // ③
            note(Role.PRIMARY, "/sync = " + target + " (artık her commit onun onayını bekliyor)");
        }
    }

    // ------------------------------------------------------------------ yardımcılar

    /** synchronous_standby_names içinde tire içeren adlar çift tırnak ister: "pg-ank" */
    static String q(String name) {
        return '"' + name + '"';
    }

    private Member bestOf(Set<String> names) {
        return etcd.members().values().stream().map(this::parse)
                .filter(m -> m != null && names.contains(m.node()))
                .max(Comparator.comparingLong(Member::lsnBytes)).orElse(null);
    }

    public Member member(String node) {
        String raw = etcd.members().get(node);
        return raw == null ? null : parse(raw);
    }

    private Member parse(String raw) {
        try {
            return json.readValue(raw, Member.class);
        } catch (Exception e) {
            return null;
        }
    }

    private String memberJson(Status st) {
        String r = st.inRecovery() ? "STANDBY" : (st.readOnly() ? "FENCED" : "PRIMARY");
        try {
            return json.writeValueAsString(new Member(me, props.region(), props.pgHost(), props.pgPort(), r, st.lsnBytes(), st.lsn()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void set(Role r, String message) {
        boolean changed = r != role || !message.equals(lastMessage);
        role = r;
        if (changed) {
            note(r, message);
        }
    }

    private void note(Role r, String message) {
        lastMessage = message;
        Event e = new Event(Instant.now(), me, r, message);
        events.add(e);
        log.info("[{}] {} — {}", me, r, message);
    }

    public Map<String, Object> status() {
        return Map.of("node", me, "region", props.region(), "role", role, "lastMessage", lastMessage);
    }
}
