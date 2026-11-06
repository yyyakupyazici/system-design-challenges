# Redis Architect Lab

**Redis'i bir mimar gözüyle:** neden atomik çalışır, hangi problemde hangi veri yapısı seçilir, cluster'da ne değişir, ve ne zaman Redis **kullanılmaz**. 14 gerçek senaryo; her biri Spring Boot kodu, gerçek bir Redis Cluster'a karşı çalışan testleri ve "neden böyle?" açıklamasıyla.

> Bu bir challenge değil, bir laboratuvar. 001 (rate limiter), 004 (bildirim kotası) ve 005 (LLM gateway bütçesi) challenge'larında Redis hep kritik yerdeydi. Bu lab o parçaları tek yerde, temelinden toparlıyor.

---

## Nasıl okunmalı?

1. **[docs/01-atomiklik.md](docs/01-atomiklik.md)**: Tek thread modeli, oku-değiştir-yaz yarışı, tek komut, Lua, MULTI, WATCH, pipelining. Her şeyin temeli bu.
2. **Senaryolar** (aşağıdaki tablo): Her birinin servisini ve testini birlikte oku. Testler davranışı kanıtlıyor.
3. **[docs/02-cluster-ve-dagitik-redis.md](docs/02-cluster-ve-dagitik-redis.md)**: Slotlar, hash tag'ler, replikasyon, gerçek failover ölçümü, Kubernetes kararları.
4. **[docs/03-mimar-karar-rehberi.md](docs/03-mimar-karar-rehberi.md)**: Problem → yapı haritası, Redis'i ne zaman kullanmamalı, eviction, kalıcılık, boyutlandırma, anti-pattern'ler, işletme kontrol listesi ve 15 soruluk kendini test et.

## 14 senaryo

| # | Senaryo | Gerçek hayattan | Redis yapısı | Atomiklik | Kod · Test |
|---|---|---|---|---|---|
| 1 | **Atomik sayaç** | Ziyaret sayısı, kampanya kontenjanı | String | `INCR`, Lua | [`atomic/`](app/src/main/java/io/paramix/redislab/atomic) · `AtomicCounterIT` |
| 2 | **Cache-aside** | Ürün detayı, profil | String + TTL | `SET NX PX` (yükleme kilidi) | [`cache/`](app/src/main/java/io/paramix/redislab/cache) · `CacheAsideIT` |
| 3 | **Dağıtık kilit + fencing** | Gece raporu tek pod'da çalışsın | String | Lua (al / bırak / uzat) | [`lock/`](app/src/main/java/io/paramix/redislab/lock) · `LockIT` |
| 4 | **Rate limit ×3** | API limiti, login denemesi, OTP | String / ZSET / Hash | Lua | [`ratelimit/`](app/src/main/java/io/paramix/redislab/ratelimit) · `RateLimitIT` |
| 5 | **Idempotency key** | Ödeme butonuna iki kez basıldı | String | `SET NX EX` | [`idempotency/`](app/src/main/java/io/paramix/redislab/idempotency) · `IdempotencyIT` |
| 6 | **Flash sale stok** | 100 iPhone, 50 bin alıcı | String + Hash | Lua | [`inventory/`](app/src/main/java/io/paramix/redislab/inventory) · `StockIT` |
| 7 | **Liderlik tablosu** | Oyun, satış yarışması | ZSET | `ZINCRBY` | [`leaderboard/`](app/src/main/java/io/paramix/redislab/leaderboard) · `LeaderboardIT` |
| 8 | **Gecikmeli + güvenilir kuyruk** | "15 dk'da ödenmezse iptal et", reaper | ZSET + Hash | Lua (claim, requeue) | [`delayedqueue/`](app/src/main/java/io/paramix/redislab/delayedqueue) · `DelayedQueueIT` |
| 9 | **Streams + consumer group + DLQ** | Sipariş olayları, faturalama | Stream | `XREADGROUP`, `XACK`, `XCLAIM` | [`streams/`](app/src/main/java/io/paramix/redislab/streams) · `StreamIT` |
| 10 | **Pub/Sub ile near cache** | Config değişince bütün pod'lar bilsin | Pub/Sub | — (ateşle-unut) | [`pubsub/`](app/src/main/java/io/paramix/redislab/pubsub) · `PubSubIT` |
| 11 | **Oturum + çevrimiçi kullanıcılar** | Login oturumu, "şu an 340 kişi online" | Hash + TTL, ZSET | Tek komut | [`presence/`](app/src/main/java/io/paramix/redislab/presence) · `PresenceIT` |
| 12 | **HyperLogLog, Bitmap, Geo** | Tekil ziyaretçi, günlük aktif, yakındaki kurye | HLL / Bitmap / Geo | Tek komut | [`probabilistic/`](app/src/main/java/io/paramix/redislab/probabilistic) · `AnalyticsIT` |
| 13 | **Sıcak key** | Viral gönderinin beğeni sayısı, kampanya config'i | Parçalı key + yerel cache | — | [`hotkey/`](app/src/main/java/io/paramix/redislab/hotkey) · `HotKeyIT` |
| 14 | **Cluster: slot, hash tag, CROSSSLOT** | Atomik birim tasarımı | — | — | [`cluster/`](app/src/main/java/io/paramix/redislab/cluster) · `ClusterIT`, `KeySlotTest` |

Her senaryonun "neden" açıklaması kendi servis sınıfının javadoc'unda, her Lua script'inin başında da neden script olması gerektiği yazıyor ([`scripts/`](app/src/main/resources/scripts)).

### Senaryolardan akılda kalacak tek cümleler

1. **Sayaç:** İki komut yazmadan önce, ikisini birden yapan tek bir komut var mı diye bak.
2. **Cache:** Stampede'e kilit, avalanche'a jitter, penetration'a negative caching. Güncellemede yazma, sil.
3. **Kilit:** Redis kilidi verimlilik içindir. Doğruluk için fencing token ya da veritabanı garantisi gerekir.
4. **Rate limit:** Sabit pencere ucuz ama sınırda 2× patlar. Kayan pencere tam ama pahalı. Token bucket dengeli.
5. **Idempotency:** İlk gelen `SET NX` ile alır. Hata verirse anahtar silinir ki tekrar denenebilsin.
6. **Stok:** "Yetiyorsa düş" tek Lua adımı. Redis hızlı kapı, sipariş DB'de, gün sonu mutabakat.
7. **Liderlik:** "Ben kaçıncıyım?" DB'de bütün tabloyu sıralamak, ZSET'te O(log N).
8. **Kuyruk:** Alınan iş silinmez, görünürlük süresiyle "processing"e taşınır. Worker ölürse iş kaybolmaz.
9. **Streams:** Mesajlar kalır, grup yerini tutar, takılan mesaj devredilir, zehirli mesaj DLQ'ya gider.
10. **Pub/Sub:** Dinlemeyen kaçırır. Sadece kaybı tolere edilebilen işlerde kullan.
11. **Oturum:** Durumu pod'dan çıkar, Redis'e koy, her istekte süresini uzat.
12. **HLL/Bitmap/Geo:** 10 bin tekil ziyaretçiyi 12 KB'ta say. 10 milyon kullanıcının aktifliğini 1,2 MB'ta tut.
13. **Sıcak key:** Düğüm eklemek çözmez. Yazmayı parçala, okumayı yerel cache ile kalkanla.
14. **Cluster:** Hash tag'i "hangi veriler birlikte atomik değişmeli?" sorusunun cevabı olarak seç.

## Hızlı başlangıç

### 1) Yerel Redis Cluster (Docker gerekmez)

```bash
./scripts/local-cluster.sh start      # 7000-7005: 3 master + 3 replika
redis-cli -p 7000 cluster nodes
```

### 2) Testler

```bash
cd app
export IT_REDIS_CLUSTER=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
mvn verify
```

Bağlantı önceliği: `IT_REDIS_CLUSTER` → `IT_REDIS_HOST`/`IT_REDIS_PORT` (tek Redis) → Docker varsa Testcontainers. Cluster'a özgü 3 test sadece gerçek cluster'da çalışır, diğer her şey tek Redis'te de geçer.

**Son sonuç** (bu repoda, yerel 6 düğümlü cluster'a karşı):

| | Test | Sonuç |
|---|---|---|
| Birim (`KeySlotTest`, Redis gerekmez) | 3 | ✓ |
| Entegrasyon, gerçek Redis Cluster | 38 | ✓ |
| Entegrasyon, tek Redis | 35 ✓, 3 atlandı (cluster'a özgü) | ✓ |

### 3) Uygulamayı çalıştır

```bash
cd app && mvn -DskipTests package
SPRING_PROFILES_ACTIVE=cluster REDIS_CLUSTER_NODES=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
  java -jar target/redis-architect-lab-0.1.0-SNAPSHOT.jar
```

Örnek istekler:

```bash
# flash sale: 2 stok, 3 alıcı
curl -X PUT  'localhost:8080/lab/stock/kulaklik?qty=2'
curl -X POST 'localhost:8080/lab/stock/kulaklik/reserve?orderId=a'   # RESERVED, kalan 1
curl -X POST 'localhost:8080/lab/stock/kulaklik/reserve?orderId=b'   # RESERVED, kalan 0
curl -X POST 'localhost:8080/lab/stock/kulaklik/reserve?orderId=c'   # 409 SOLD_OUT

# idempotency: aynı anahtarla ikinci istek aynı ödemeyi döner
curl -X POST 'localhost:8080/lab/payments?amountKurus=15000' -H 'Idempotency-Key: k-1'
curl -X POST 'localhost:8080/lab/payments?amountKurus=15000' -H 'Idempotency-Key: k-1'

# rate limit: fixed | sliding | bucket — 10 istekten sonra 429
for i in $(seq 1 12); do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/lab/ratelimit/sliding/ali; done

# cluster: topoloji ve key'lerin slotları
curl 'localhost:8080/lab/cluster?keys=cart:{user42},orders:{user42},cart:user42'
```

Bütün uçlar: [`api/LabController.java`](app/src/main/java/io/paramix/redislab/api/LabController.java).

### 4) docker-compose (6 düğüm + uygulama)

```bash
docker compose up --build
```

### 5) Kubernetes (kind)

```bash
kind create cluster --name redis-lab --config k8s/kind-config.yaml
docker build -t paramix/redis-architect-lab:0.1.0 app && kind load docker-image paramix/redis-architect-lab:0.1.0 --name redis-lab
kubectl apply -k k8s/
kubectl -n redis-lab wait --for=condition=complete job/redis-cluster-init --timeout=180s
kubectl -n redis-lab port-forward svc/redis-lab-app 8080:80
curl localhost:8080/lab/cluster
```

Manifest'lerdeki her kararın (StatefulSet, headless Service, anti-affinity, PDB, announce-hostname, `noeviction`, NetworkPolicy...) sebebi [docs/02 §5](docs/02-cluster-ve-dagitik-redis.md)'te.

## Gerçek failover denemesi

Uygulama çalışırken, sayaç key'inin sahibi olan master'ı öldürdük (ayrıntılar [docs/02 §4](docs/02-cluster-ve-dagitik-redis.md)):

| | Değer |
|---|---|
| Sunucunun failover süresi (`cluster-node-timeout 5000`) | ~9,5 sn |
| Uygulamanın toparlanması, Lettuce `refresh.period: 30s` | ~30 sn |
| Uygulamanın toparlanması, Lettuce `refresh.period: 5s` | **~9 sn** |
| Kaybolan artış (düşük trafik) | 0 |

**Ders:** failover süresi, sunucunun failover'ı **artı** istemcinin bunu fark etmesi. İkincisi çoğu zaman gözden kaçar.

## Klasör yapısı

```
redis-architect-lab/
├── README.md
├── docs/
│   ├── 01-atomiklik.md
│   ├── 02-cluster-ve-dagitik-redis.md
│   └── 03-mimar-karar-rehberi.md
├── design/                    k8s topolojisi, atomiklik karar ağacı (Mermaid)
├── scripts/local-cluster.sh   Docker'sız 6 düğümlü cluster
├── docker-compose.yml
├── k8s/                       namespace, secret, redis.conf, StatefulSet ×6, init Job, uygulama, NetworkPolicy, kind
└── app/                       Spring Boot 3.5 / Java 21 / Lettuce
    └── src/main/resources/scripts/   14 Lua script'i, her birinin başında "neden script?"
```

## Kapsam dışı ve dürüst notlar

- **Doğrulanmadı:** `docker-compose.yml` ve `k8s/` manifest'leri bu ortamda Docker ve Kubernetes olmadığı için çalıştırılmadı, sadece YAML sözdizimi kontrol edildi. Kod ve testler gerçek bir Redis Cluster'da (yerel süreçler) doğrulandı.
- **Kubernetes'te bütün pod'ların aynı anda yeniden doğması:** pod IP'lerinin hepsi değişirse düğümler birbirini bulamayabilir (docs/02 §5). Üretimde bir operator ya da olgun bir Helm chart tercih edilir. Buradaki manifest'ler öğretmek için açık yazıldı.
- **Modül gerektirenler:** Bloom filter (RedisBloom), tam metin ve vektör arama (RediSearch) anlatıldı ama uygulanmadı.
- **Sentinel:** docs/02'de karşılaştırıldı, ayrı bir kurulum yok.
