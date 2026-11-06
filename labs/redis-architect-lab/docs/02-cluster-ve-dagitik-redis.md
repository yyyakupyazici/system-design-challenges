# 02 · Cluster ve dağıtık Redis

Bu doküman Redis'i tek bir sunucudan çıkarıp birden çok düğüme yaydığında neyin değiştiğini anlatır: slotlar, hash tag'ler, replikasyon, failover ve Kubernetes'te dikkat edilmesi gerekenler. Ölçümler bu labdaki yerel 6 düğümlü cluster'da yapıldı.

## 1. Üç dağıtım modeli

| Model | Ne | Ölçek | Yüksek erişilebilirlik | Ne zaman |
|---|---|---|---|---|
| **Tek düğüm + replika** | 1 master, N replika (okuma kopyası) | Tek düğümün RAM'i ve CPU'su | Yok, failover elle | Küçük sistemler, cache |
| **Sentinel** | 1 master + replikalar, Sentinel süreçleri izler ve failover yapar | Tek düğümün RAM'i ve CPU'su | ✓ otomatik failover | Veri tek düğüme sığıyor, otomatik failover isteniyor |
| **Cluster** | Veri 16.384 slota bölünür, slotlar master'lara dağıtılır, her master'ın replikası olur | Düğüm ekledikçe RAM ve yazma kapasitesi artar | ✓ otomatik failover | Veri ya da yazma yükü tek düğümü aşıyor |

**Kural:** Cluster'ın tek gerçek sebebi **tek düğümün yetmemesi**dir (RAM ya da yazma hızı). Sadece yüksek erişilebilirlik için cluster kurmak gereksiz karmaşıklık getirir. Sentinel yeterlidir. Bu lab cluster'ı öğretmek için cluster kullanıyor, ve bütün kod tek düğümde de çalışıyor (testler ikisinde de geçiyor).

## 2. Slotlar ve hash tag'ler

```
slot = CRC16(key) mod 16384
```

Her master bir slot aralığının sahibidir. Bu labdaki cluster:

```
127.0.0.1:7000  master  0-5460
127.0.0.1:7001  master  5461-10922
127.0.0.1:7002  master  10923-16383
127.0.0.1:7003..7005    replikalar
```

`common/KeySlot.java` bu hesabı sunucuya sormadan yapar. `ClusterIT.ourSlotCalculationMatchesTheServer` sonucun `CLUSTER KEYSLOT` ile aynı olduğunu doğrular.

**Hash tag:** Key'de `{...}` varsa sadece süslü parantezin içi hash'lenir:

```
cart:{user42}     → slot( "user42" )  ┐
orders:{user42}   → slot( "user42" )  ┘ aynı slot, aynı düğüm
cart:user42       → slot( "cart:user42" )   ┐
orders:user42     → slot( "orders:user42" ) ┘ büyük ihtimalle farklı düğümler
```

### Neden önemli?

Birden çok key'e dokunan her işlem **key'lerin aynı slotta olmasını** ister:
- Lua script'leri (`KEYS[1]`, `KEYS[2]`...)
- `MULTI`/`EXEC`
- `MGET`/`MSET` (Spring bunları cluster'da düğüm düğüm böler, ama o zaman atomik değildir)
- `PFMERGE`, `BITOP`, `SINTERSTORE`, `ZUNIONSTORE`, `RPOPLPUSH`, `LMOVE`...

Değilse Redis `CROSSSLOT Keys in request don't hash to the same slot` hatası verir. `ClusterIT.multiKeyScriptFailsAcrossSlotsAndWorksWithAHashTag` bunu gerçek cluster'da gösteriyor.

### Hash tag tasarımı = atomiklik birimi tasarımı

Hash tag seçerken aslında şu soruyu cevaplıyorsun: **"Hangi verilerin birlikte, atomik olarak değişmesi gerekiyor?"**

| Senaryo | Hash tag | Birlikte değişenler |
|---|---|---|
| Flash sale | `{sku}` | Stok + o ürünün siparişleri |
| Kilit | `{kaynak}` | Kilit + fencing sayacı |
| Gecikmeli kuyruk | `{kuyruk}` | ready + processing + jobs |
| Bütçe (005) | `{ekip:gün}` | Ekip toplamı + kullanıcılar + rezervasyonlar |
| Günlük analitik | `{sayfa}` / `{uygulama}` | Günlük HLL'ler / bitmap'ler (birleştirme için) |

**Tuzak:** Hash tag'i çok geniş seçersen (örneğin her şey `{global}`) bütün veri tek bir slota, tek bir düğüme düşer. Cluster'ın hiçbir faydası kalmaz ve o düğüm sıcak olur. Tag, atomik olması gereken **en küçük** birim olmalı.

### Sıcak key: hash tag'in tersi

Bazen tam tersini istersin: tek bir mantıksal değeri **bilerek** farklı slotlara yaymak. `hotkey/ShardedCounter` bir sayacı `counter:{begeni#0}` … `counter:{begeni#7}` olarak 8 parçaya böler, her parçanın tag'i farklı olduğu için parçalar farklı düğümlere dağılır. Bedeli, okumanın 8 key'i toplaması ve toplamın atomik olmaması.

## 3. MOVED, ASK ve "akıllı istemci"

İstemci yanlış düğüme sorarsa düğüm cevap vermez, yönlendirir:

```
istemci → 7000: GET foo
7000 → istemci: MOVED 12182 127.0.0.1:7002
istemci → 7002: GET foo
```

Lettuce gibi cluster-aware istemciler başta bir düğüme bağlanıp `CLUSTER SLOTS` ile bütün topolojiyi öğrenir. Her key'i doğrudan doğru düğüme gönderir. `MOVED` sadece topoloji değiştiğinde (failover, resharding) görülür, ve istemci o zaman topolojiyi yeniler.

`ASK` geçicidir: bir slot bir düğümden diğerine taşınırken (resharding), taşınmış key'ler için kullanılır.

## 4. Replikasyon ve failover

### Replikasyon asenkron

Master bir yazmayı **önce kendi belleğine yazar, istemciye "tamam" der, sonra** replikaya gönderir. Master tam o arada ölürse:

```
istemci → master: SET NX lock:{x} ...    → "OK"
master  → (replikaya göndermeden ölür)
replika → yeni master olur, lock:{x}'i hiç görmedi
başka istemci → yeni master: SET NX lock:{x} → "OK"   ← iki kişi aynı kilitte
```

Bu yüzden:
- Redis kilidi **verimlilik** kilidi olarak kullanılır, **doğruluk** için kilit + fencing token ya da veritabanının kendi garantisi gerekir (`lock/RedisLock` javadoc'u ve `LockIT.fencingTokenStopsTheZombieOwnersLateWrite`).
- Stok, idempotency, bütçe gibi verilerde Redis hızlı kapı, gerçeğin kaynağı veritabanı. Gün sonunda mutabakat yapılır.

`WAIT numreplicas timeout` komutu, o ana kadarki yazmaların en az N replikaya ulaşmasını bekler. Kayıp penceresini daraltır ama tam garanti değildir: replika yazmayı almış olsa da failover'da en güncel replikanın seçileceği garanti edilmez. Redis güçlü tutarlılık (strong consistency) sağlamaz.

### Failover zaman çizelgesi (bu labda ölçüldü)

`cluster-node-timeout 5000` ile, `counter:{failover}` key'inin sahibi olan master'ı (`7000`) `shutdown nosave` ile öldürdük. Uygulama her 250 ms'de bir sayacı artırmaya devam etti.

| An | Ne oldu |
|---|---|
| t = 0 | Master öldürüldü. O slot aralığına yazmalar hata vermeye başladı |
| t ≈ 5 sn | Diğer düğümler master'ı `PFAIL` (şüpheli) olarak işaretledi |
| t ≈ 9,5 sn | Çoğunluk `FAIL` dedi, replika seçimi kazandı: *"Failover election won: I'm the new master"* |
| t ≈ 9,5 sn + istemci | Uygulama yeni master'ı öğrenince yazmalar yeniden başladı |

İstemci ayarının etkisi:

| Lettuce `refresh.period` | Uygulamanın toparlanma süresi |
|---|---|
| 30 sn | ~30 sn (sunucu failover'ı bitirdikten ~20 sn sonra) |
| **5 sn** | **~9 sn** (sunucunun kendi failover süresi kadar) |

Ders: **failover süresi = sunucunun failover'ı + istemcinin bunu fark etmesi.** İkincisi çoğu zaman gözden kaçar. `application.yml`'daki `adaptive: true` ve `period: 5s` bu yüzden var.

Her iki denemede de sayaç değeri başarılı istek sayısıyla birebir tuttu, kayıp yazma görmedik. Bu düşük trafikli bir denemeydi. Yüksek trafikte, master'ın öldüğü an replikaya ulaşmamış yazmalar kaybolabilir (yukarıdaki asenkron replikasyon bölümü).

### Kendin dene

```bash
./scripts/local-cluster.sh start
redis-cli -p 7000 cluster keyslot "counter:{failover}"        # hangi slot?
redis-cli -p 7000 cluster nodes                                # o slotun sahibi kim?
redis-cli -p <sahip> shutdown nosave                           # öldür
watch -n1 'redis-cli -p 7001 cluster nodes | sort'             # replikanın master olmasını izle
```

## 5. Kubernetes'te Redis Cluster

`k8s/` altındaki manifest'lerdeki her kararın sebebi:

| Karar | Neden |
|---|---|
| **StatefulSet** | Her pod'un sabit adı (`redis-cluster-0..5`), sabit DNS kaydı ve kendi diski (PVC) olur. Pod yeniden doğduğunda aynı diski ve aynı düğüm kimliğini (`nodes.conf`) alır |
| **Headless Service** | Pod başına DNS kaydı. Düğümler ve istemciler birbirini adıyla bulur |
| `publishNotReadyAddresses: true` | Cluster kurulurken düğümler henüz "hazır" değilken de birbirini bulabilmeli |
| `cluster-announce-hostname` + `cluster-preferred-endpoint-type hostname` | Pod IP'leri her yeniden doğuşta değişir, DNS adları değişmez. İstemci yönlendirmelerde IP yerine adı alır |
| `nodes.conf` içindeki IP'yi güncelleyen başlangıç komutu | Düğüm yeni IP ile açıldığında kendi kaydını düzeltir, diğerleri gossip ile öğrenir. **Sınır:** bütün pod'lar aynı anda yeniden doğarsa (bütün IP'ler değişirse) düğümler birbirini bulamayabilir. Üretimde bunun için bir operator ya da hazır bir Helm chart tercih edilir |
| **Pod anti-affinity** | Bir master ile replikası aynı node'daysa, node gidince ikisi birden gider ve o slot aralığı veriyle birlikte kaybolur |
| **PDB `maxUnavailable: 1`** | Node drain sırasında aynı anda iki Redis pod'u gitmesin |
| `liveness failureThreshold: 6` | Liveness probe failover'dan (5 sn) sabırlı olmalı. Yoksa Kubernetes, yavaşlayan bir Redis'i gereksiz yere öldürür ve failover'a yol açar |
| Bellek limiti > `maxmemory` | AOF rewrite ve RDB snapshot `fork()` kullanır, ve copy-on-write sırasında bellek geçici olarak artar. Limit aşılırsa OOMKill olur, o da failover demek |
| **Init Job** | `redis-cli --cluster create` bir kez çalışır. Cluster zaten kuruluysa hiçbir şey yapmaz, yeniden çalıştırmak güvenli |
| **NetworkPolicy** | Redis'e sadece uygulama ve init Job erişir. 16379 (cluster bus) sadece Redis pod'ları arasında açık |
| `rename-command KEYS ""`, `FLUSHALL ""` | Üretimde bir kişinin yanlışlıkla bütün cluster'ı kilitlemesini ya da silmesini imkânsız kılar |

## 6. Ölçeklendirme: düğüm ekleme ve resharding

```bash
redis-cli --cluster add-node <yeni>:6379 <mevcut>:6379
redis-cli --cluster rebalance <mevcut>:6379 --cluster-use-empty-masters
```

Slotlar yeni master'a taşınırken (`ASK` yönlendirmeleriyle) cluster hizmet vermeye devam eder. Büyük key'ler (milyonlarca elemanlı ZSET gibi) taşınırken o key üzerinde gecikme yaşanır: büyük key'lerden kaçınmanın bir sebebi de bu (bkz. [03](03-mimar-karar-rehberi.md)).
