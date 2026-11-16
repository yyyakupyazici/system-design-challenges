# Raft & etcd Lab: PostgreSQL'i iki bölgede güvenle çalıştırmak

012'deki (iki bölge, aktif-aktif ve felaket kurtarma) tartışmanın canlı hali. Konuştuğumuz her adımı gerçek parçalarla burada görebilirsin:

- 3 düğümlü bir **etcd** kümesi (İstanbul, Ankara ve tanık), içinde Raft çalışıyor.
- 4 **PostgreSQL** ve aralarında senkron replikasyon.
- Her PostgreSQL'in yanında bir **mini-Patroni** (Spring Boot): lease, compare-and-set, watch, fencing ve promote işlerini kendisi yapıyor. Kod kısa ve her kararı Türkçe logluyor.

Her ders bir entegrasyon testi. Testler süreç öldürüyor, donduruyor, bağlantı kesiyor ve ne olduğunu anlatarak raporluyor.

```
            İstanbul                         Ankara                    Tanık (3. yer)
   ┌───────────────────────────┐   ┌───────────────────────────┐   ┌───────────────┐
   │ ajan ── pg-ist  (PRIMARY) │   │ ajan ── pg-ank  (SYNC)    │   │               │
   │ ajan ── pg-ist2 (standby) │   │ ajan ── pg-ank2 (standby) │   │               │
   │          │                │   │          │                │   │               │
   │      etcd-ist ◄───────────┼───┼──► etcd-ank ◄────────────┼───┼──► etcd-wit   │
   └───────────────────────────┘   └───────────────────────────┘   └───────────────┘
                      └──────────── Raft: 3 oy, çoğunluk 2 ────────────────┘

   etcd'deki kayıtlar:
     /paramix/ledger/leader          = pg-ist     (6 sn'lik kiraya bağlı: yenilenmezse SİLİNİR)
     /paramix/ledger/sync            = pg-ank     ("tamam" denen her commit'e sahip olan: failover'da SADECE o)
     /paramix/ledger/members/{node}  = {rol, LSN, bölge, port}
```

## Hızlı başlangıç (kendin dene)

```bash
cd labs/raft-etcd-lab
./scripts/lab.sh up                    # 3 etcd + 4 PostgreSQL (/var/tmp/raftlab), ~5 sn
./scripts/lab.sh status                # Raft term/index, etcd kayıtları, PostgreSQL rolleri

cd app && mvn -q -DskipTests package && JAR=$(ls target/raft-etcd-lab-*.jar)
NODE=pg-ist  REGION=istanbul PG_PORT=5511 PORT=8091 java -jar $JAR &     # İstanbul önce: ilk lider o olsun
sleep 3
NODE=pg-ank  REGION=ankara   PG_PORT=5512 PORT=8092 java -jar $JAR &
NODE=pg-ist2 REGION=istanbul PG_PORT=5513 PORT=8093 java -jar $JAR &
NODE=pg-ank2 REGION=ankara   PG_PORT=5514 PORT=8094 java -jar $JAR &

psql -h 127.0.0.1 -p 5511 -U postgres ledger -c "INSERT INTO accounts VALUES ('ali', 100000), ('ayse', 0)"
curl -X POST localhost:8092/v1/transfers -H 'Content-Type: application/json' -d '{"from":"ali","to":"ayse","amount":2500}'
curl localhost:8091/events            # ajanın karar zaman çizelgesi

# Felaketler:
../scripts/lab.sh kill etcd-ist        # Raft lideri ölürse? (status'ta term artışını izle)
../scripts/lab.sh pause etcd-ist       # İstanbul çoğunluktan koptu (dondurma = ağ kesildi gibi)
../scripts/lab.sh kill pg-ist          # İstanbul'daki primary öldü
watch -n1 ../scripts/lab.sh status
```

> Ajanlar ve script `root` ya da `postgres` kullanıcısıyla çalışmalı (PostgreSQL süreçleri `postgres` kullanıcısıyla açılıyor). Gereken: `etcd`/`etcdctl` (3.4+) ve PostgreSQL 13+ ikili dosyaları.

## Dersler

`cd app && mvn verify` hepsini sırayla çalıştırır (~2 dk). Her testin anlatımı `app/target/failsafe-reports/*-output.txt` dosyasına düşer. Aşağıdaki çıktılar o dosyalardan alındı.

### Ders 1–2: Üç makinede aynı sıralı liste (`Ders1_2_RaftLogIT`)

Ortak bir disk yok. Her düğümün kendi log'u var ve Raft hepsini **aynı sırada** tutuyor. `raftIndex` = log'un kaçıncı satırına kadar yazıldığı.

```
   ┌ 10 yazmadan sonra                    ┌ etcd-ist donmuşken 5 yazma daha        ┌ yetiştikten sonra
   │ etcd-ist  takipçi term=2 raftIndex=18  │ etcd-ist  ulaşılamıyor                  │ etcd-ist  takipçi term=2 raftIndex=23
   │ etcd-ank  takipçi term=2 raftIndex=18  │ etcd-ank  takipçi term=2 raftIndex=23   │ etcd-ank  takipçi term=2 raftIndex=23
   │ etcd-wit  LİDER  term=2 raftIndex=18   │ etcd-wit  LİDER  term=2 raftIndex=23    │ etcd-wit  LİDER  term=2 raftIndex=23
```

- 10 yazma → üç düğümde de 8'den 18'e. **Aynı sıralı liste, üç makine.**
- Bir takipçi donukken yazmalar **yine başarılı**: lider + 1 takipçi = 2/3 çoğunluk = commit.
- Donmuş düğüm çözülünce lider ona eksik satırları gönderir, o da yetişir.

### Ders 3–5: Lider ölünce ve çoğunluk kaybolunca (`Ders3_5_RaftElectionIT`)

```
▶ Yeni lider etcd-wit, term 2 → 3. Seçim 1630 ms sürdü (election-timeout 1000 ms)
▶ ③ etcd-ank de öldürülüyor: geriye tek düğüm kaldı = 1/3, çoğunluk YOK
▶    yazma: ZAMAN AŞIMI · doğrusal okuma: HATA · serializable okuma: 'kesin-6' (kendi kopyası)
   ┌ toparlandıktan sonra:  term=8, raftIndex=19 üçünde de
```

- **Yeni seçim = yeni dönem (term).** Aynı dönemde iki lider olamaz: her düğüm bir dönemde tek oy verir.
- Commit edilmiş 5 yazmanın hepsi yeni liderde var. Çoğunluk onayından geçen hiçbir şey kaybolmaz, çünkü iki çoğunluk mutlaka en az bir düğümde kesişir.
- **1/3 kalınca:** yazma durur, doğrusal okuma da durur ("hâlâ lider miyim?" teyidi çoğunluk ister). `serializable` okuma düğümün kendi kopyasından cevap verir, yani **eski olabilir**.
- **Term neden 3'ten 8'e çıktı?** Tek kalan düğüm çoğunluk bulamadığı için her election-timeout'ta yeni bir seçim başlattı (her seferinde term+1) ve hepsini kaybetti. Geri dönen düğümler en büyük term'i benimser.
- **Zaman aşımı alan yazma ne oldu?** Testin iki koşusunda iki farklı sonuç çıktı ve ikisi de doğru. Tek kalan düğüm yazmayı log'una **eklemişti** ama commit edememişti. Diğerleri dönünce:
  - Log'unda o satır olmayan bir düğüm lider olursa satır **silinir** (yazma hiç olmamış olur).
  - Satırı tutan düğüm yeniden lider olursa onu kopyalar ve yazma **sonradan kesinleşir**.

  İstemci açısından: **zaman aşımı "başarısız" değil, "sonuç bilinmiyor" demek.** Tekrar denemek ancak idempotency anahtarıyla güvenli olur (006, 008).

### Ders 6: Patroni'nin etcd'den kullandığı araçlar (`Ders6_EtcdPrimitivesIT`)

| Araç | Test ne gösteriyor | Ajanda nerede |
|---|---|---|
| **Lease (TTL)** | Yenilenen anahtar kalır, yenilenmeyen 3 sn sonra **kendiliğinden** silinir | `EtcdStore.grantLease/keepAlive`: lider kaydı kiraya bağlı |
| **Txn (compare-and-set)** | 10 aday aynı anda "anahtar YOKSA beni yaz" diyor → **tam 1 kazanan** | `EtcdStore.tryAcquireLeader`: `If(createRevision == 0) Then(put)` |
| **Watch** | Kira dolunca DELETE olayı 2004 ms'de geldi (kira 2 sn'di). Sormaya gerek yok | `HaAgent.start`: lider silinince hemen döngü |
| **Revision** | Her yazmada artan sayı → **fencing token** (Redis Lab'deki gibi) | Lider kaydının `createRevision`'ı loglanıyor |

### Ders 7: İstanbul yanıyor (`Ders7_FailoverIT`)

50 transfer "tamam" aldı. Sonra pg-ist, pg-ist2, etcd-ist ve iki ajan **aynı anda** öldürüldü.

```
▶ pg-ank lider oldu: RTO ≈ 6–8,5 sn (≈ ttl + bir döngü). pg-ank2 /sync'te olmadığı için aday OLMADI
▶ pg-ank'de 50/50 transfer var, toplam bakiye 10000000 (başta 10.000.000)
▶ ⑤ Yeni lider Ankara'daki diğer kopyayı (pg-ank2) senkron yapınca yazmaya RPO 0 ile devam

[pg-ank2] STANDBY  lider yok ama aday olamam: /sync = [pg-ank] ("tamam" denen her şeyin bende olduğu garanti değil)
[pg-ank]  STANDBY  lider yok. Aday oluyorum: /sync = [pg-ank] içindeyim: "tamam" denen her commit bende
[pg-ank]  PRIMARY  lider kaydını ALDIM (compare-and-set başarılı)
[pg-ank]  PRIMARY  PostgreSQL promote edildi: artık yazma kabul ediyorum
[pg-ank]  PRIMARY  synchronous_standby_names = "pg-ank2"
```

- etcd-ank + etcd-wit = 2/3. Pano çalışmaya devam ediyor.
- Lider kaydının kirası ttl (6 sn) sonunda düştü. pg-ank compare-and-set ile kaydı aldı ve promote edildi.
- **RPO 0:** "tamam" denen her commit pg-ank'te. Primary, "tamam" demeden önce onun "WAL diskimde" demesini beklemişti.
- **Neden Ankara'da iki PostgreSQL var?** Strict modda tek kopyayla "tamam" denmez. İlk denemede Ankara'da tek düğüm vardı ve İstanbul yandıktan sonra **bütün commit'ler bekledi**. RPO 0'ı failover'dan sonra da sürdürmek için her bölgede iki kopya gerekiyor.

### Ders 8: Split-brain olmuyor (`Ders8_SplitBrainIT`)

İstanbul çoğunluktan koptu (etcd-ist donduruldu), ama İstanbul'daki PostgreSQL sapasağlam. İki "uygulama" durmadan yazıyor: biri **doğrudan pg-ist'e**, biri **doğrudan pg-ank'e**.

```
▶ pg-ist'e son başarılı yazma:   t=+2596 ms
▶ pg-ist ilk ret:                t=+2701 ms → ERROR: cannot execute UPDATE in a read-only transaction
▶ pg-ank'e ilk başarılı yazma:   t=+6072 ms
▶ Arada kimsenin yazamadığı boşluk: 3476 ms. İki primary'nin aynı anda yazdığı an: YOK

[pg-ist] PRIMARY  etcd'ye ulaşamıyorum (1522 ms), kiram hâlâ geçerli olabilir, bekliyorum
[pg-ist] FENCED   etcd çoğunluğuna 3030 ms'dir ulaşamıyorum, kiram her an düşebilir → KENDİMİ YAZMAYA KAPATTIM
[pg-ank] PRIMARY  lider kaydını ALDIM (compare-and-set başarılı)
[pg-ist] FENCED   lider pg-ank, ben eski primary'yim → yazmaya kapalıyım (geri katılmak için pg_rewind gerekir)
```

Zamanlama kuralı:
```
eski lider kendini kapatır:  en geç ttl/2   (3 sn)    ← ajan kendi saatine göre, temkinli
yeni lider seçilebilir:      en erken ttl   (6 sn)    ← kira etcd'de ancak o zaman düşer
                             └── arada kimse yazmıyor: kısa kesinti, çift harcamadan iyidir
```

etcd-ist geri geldiğinde pg-ist ajanı panoda "lider pg-ank" görür ve **kendini açmaz**.

### Ders 9a: Strict senkronun bedeli (`Ders9a_StrictSyncIT`)

```
▶ ① Senkron standby pg-ank donuyor
▶    2 sn geçti: commit hâlâ bekliyor (istemci cevap almadı)
▶ ③④ 6018 ms sonra 'tamam': ajan pg-ank2'yi senkron yaptı, /sync = [pg-ank2]
```

Senkron standby cevap vermezse commit **bekler**. `wal_sender_timeout` (5 sn) sonra primary onu düşürür, ajan da Ankara'daki diğer kopyaya geçer. Değişiklik sırası Patroni'deki gibi:
1. `/sync`'i genişlet (`{pg-ank, pg-ank2}`)
2. `synchronous_standby_names`'i değiştir
3. Yeni aday gerçekten `sync` olunca `/sync`'i daralt

`/sync` bir **söz**: "burada adı geçen, 'tamam' denen her commit'e sahip." Söz, PostgreSQL gerçekten öyle çalışmadan daraltılmaz.

### Ders 9b: "ANY 1" tuzağı (`Ders9b_AnyOneTrapIT`)

`synchronous_standby_names = ANY 1 ("pg-ank", "pg-ank2", "pg-ist2")`: herhangi bir standby onaylayınca "tamam".

```
▶ ① Ankara'nın iki standby'ı donuyor (hat koptu). Transfer yine de 'tamam' alıyor: onay pg-ist2'den
▶    transfer 1 'tamam' aldı (8 ms). Ali'nin ekranında '✓ 999 TL gönderildi'
▶ ② İstanbul yanıyor: pg-ist, pg-ist2, etcd-ist, iki ajan
▶ ④ Yeni lider pg-ank2. 'tamam' denen transfer 1 orada mı? HAYIR, KAYBOLDU
```

Onay, primary ile **aynı binadaki** standby'dan geldi; bina yanınca ikisi birlikte gitti. Sistem bunu fark etmedi bile: Ankara `/sync`'te adı olduğu için lider oldu.

> **Kural "kaç kopya" değil, "kopyalar nerede".** Onay, felaketin götüremeyeceği bir yerden gelmeli. Ajanın SINGLE modu senkron standby'ı bu yüzden **başka bölgeden** seçiyor.

### Ders 10: Yazma primary'ye, okuma standby'a (`Ders10_ReadYourWritesIT`)

pg-ist2'ye yapay 2 sn gecikme verildi (`recovery_min_apply_delay`).

```
▶ ① Transfer 250 TL → primary pg-ist, commit LSN 0/500B7A8
▶ ② pg-ist2'den düz okuma: Ayşe'nin bakiyesi 0 (replay 0/500B780) → ESKİ. Para kaybı yok, ekran geride
▶ ③ LSN şartıyla pg-ist2: 1025 ms bekledi, yetişemedi → pg-ist'den okundu: 25000
▶ ④ LSN şartıyla pg-ank (senkron standby): 9 ms bekledi, kendisi cevap verdi: 25000
```

- **Gösterilen okuma** (bakiye ekranı, geçmiş) standby'dan yapılabilir. "Kendi yazdığını görsün" için commit LSN'i taşınır: standby o LSN'i uygulamadıysa kısa süre beklenir, yetişemezse primary'den okunur.
- **Karar veren okuma** ("bakiye yeterli mi?") hiç standby'a gitmez. Transfer içinde, primary'de koşullu `UPDATE ... WHERE balance >= ?` ile yapılır.

## Mini-Patroni nasıl okunur

`app/src/main/java/io/paramix/raftlab/`

| Dosya | İş |
|---|---|
| `ha/HaAgent.java` | **Bekçi döngüsü.** Her `loop-wait`'te: lider benim / başkası / yok / pano cevap vermiyor. Her durumun kararı Türkçe loglu |
| `etcd/EtcdStore.java` | İlan panosu: lease, compare-and-set ile lider olma, `/sync` (sadece hâlâ liderken yazılır), watch |
| `pg/PgNode.java` | Gerçek PostgreSQL komutları: `pg_promote()`, fence (`default_transaction_read_only`), `synchronous_standby_names`, `primary_conninfo`, `pg_stat_replication` |
| `ledger/LedgerService.java` | HAProxy'nin işi: yazma lidere gider, okuma LSN şartıyla standby'a |
| `api/LabController.java` | `/primary`, `/replica` (Patroni'deki gibi sağlık uçları), `/events`, transfer ve bakiye uçları |

```
lider BENİM          → kirayı uzat. Uzatamazsam ve son başarılı uzatma ttl/2'den eskiyse: KENDİMİ KAPAT
lider BAŞKASI        → standby'sam: onu takip et (primary_conninfo). Primary'sem: eski primary'yim → kendimi kapat
lider YOK            → standby'sam sadece /sync'te adım varsa aday olurum → "yoksa yaz" txn → kazanırsam promote
pano CEVAP VERMİYOR  → liderdiysem ve ttl/2 geçtiyse kendimi kapat; değilsem hiçbir karar verme
```

## Yapılırken yakalanan gerçek hatalar (her biri bir ders)

| Ne oldu | Sebep | Ders |
|---|---|---|
| Ajanları yeniden başlatınca Ankara lider oldu, İstanbul eski primary olarak kaldı | Ajanlar kapalıyken lider kaydının kirası düştü; `/sync`'te Ankara vardı | Doğru davranış. Ama **ajan ölünce PostgreSQL korumasız kalır**: kimse onu kapatmaz. Patroni bu yüzden **watchdog** (`/dev/watchdog`) kullanır: Patroni ölürse makine yeniden başlatılır |
| İlk senkron standby İstanbul'daki pg-ist2 oldu ve öyle kaldı | Önce bağlanan seçiliyordu, sonra gelen daha iyi aday görülmüyordu | Senkron aday "başka bölge önce" kuralıyla sürekli yeniden değerlendirilmeli. Yoksa İstanbul yanınca "tamam" denen veri Ankara'da olmaz |
| Yeni lider ölü bir düğümü senkron aday seçti, commit'ler takıldı | etcd'deki üye kaydı, kirası düşene kadar (ttl) ölü düğümü de gösteriyor | Aday listesi `pg_stat_replication`'dan, yani **gerçekten bağlı** olanlardan çıkarılmalı |
| pg-ank hiç replike olmadı: `requested WAL segment ... has already been removed` | Yedek alındıktan sonra, standby bağlanmadan primary eski WAL'ı sildi | `wal_keep_size` (lab) ya da üretimde **replication slot** |
| `ANY 1` tuzağı ilk denemede görünmedi | Süreci dondurmak (SIGSTOP) bağlantıyı kesmiyor: WAL donmuş sürecin TCP tamponunda bekledi, çözülünce okundu | Redis Lab'deki "eski master" deneyinin aynısı. Gerçek hat kopukluğunu taklit etmek için bağlantıyı da kes |
| `lab.sh up` 96 sn sürüyordu | `pg_basebackup` varsayılan olarak yavaş (spread) checkpoint bekliyor | `-c fast` → 4 sn |

## Lab ile gerçek Patroni arasındaki farklar

| Konu | Lab | Patroni / üretim |
|---|---|---|
| Fence | `default_transaction_read_only = on` + bağlantıları kes | PostgreSQL'i durdurup standby olarak yeniden başlatır (demote). Bir istemci `SET TRANSACTION READ WRITE` ile lab'in fence'ini aşabilir |
| Ajan ölürse | PostgreSQL korumasız | Watchdog makineyi yeniden başlatır |
| Eski primary'nin geri katılması | Elle (`pg_rewind` + standby olarak başlatma) | Patroni `pg_rewind`'i otomatik çalıştırır |
| WAL saklama | `wal_keep_size` | Her üye için replication slot |
| etcd'ye ulaşılamayınca | Lider kendini kapatır | Aynı. Patroni 3.x'te ek olarak "failsafe mode": bütün üyelere REST ile ulaşabiliyorsa primary açık kalabilir |
| Kubernetes'te | `k8s/etcd.yaml` (sadece etcd) | CloudNativePG ya da Zalando Postgres Operator (Patroni). İkisi de bu dersteki mantığı uygular |

## Dürüst notlar

- Her şey tek makinede çalışıyor; "bölgeler" port numaralarından ibaret. Gerçek ağ gecikmesi (İstanbul–Ankara ~8 ms) yok, bu yüzden commit gecikmesi ölçümleri gerçeği yansıtmaz.
- Split-brain deneyi bir basitleştirme kullanıyor: her ajan sadece kendi bölgesinin etcd'sine bağlı. Gerçekte ajanlar bütün etcd uçlarını bilir; bölünmeyi ağ kuralları (iptables / network namespace) yaratır.
- `k8s/etcd.yaml` ve `app/Dockerfile` bu ortamda çalıştırılmadı (Docker ve Kubernetes yok). Sadece YAML sözdizimi kontrol edildi.
- **Test sonucu:** `mvn verify` → 1 birim + 11 entegrasyon testi geçti (etcd 3.4.30, PostgreSQL 16; tam koşu ~2 dk). etcd ya da PostgreSQL ikili dosyaları yoksa entegrasyon testleri atlanır.
