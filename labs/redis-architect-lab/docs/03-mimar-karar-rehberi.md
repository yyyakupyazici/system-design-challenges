# 03 · Mimar karar rehberi: Redis nerede, nasıl, neden?

Bu doküman bir tasarım toplantısında sorulacak soruların cevap anahtarı: Redis'i ne zaman kullanırsın, hangi yapıyı seçersin, hangi ayarlarla çalıştırırsın, ve ne zaman **kullanmazsın**.

## 1. Redis'i seçmeden önce üç soru

1. **Bu veri kaybolursa ne olur?**
   - Hiçbir şey, yeniden hesaplanır (cache) → Redis ideal.
   - Biraz rahatsızlık (oturum düşer, rate limit birkaç saniye gevşer) → Redis uygun.
   - Para ya da güven kaybı (bakiye, sipariş) → Redis **hızlı kopya** olabilir, gerçeğin kaynağı olamaz.
2. **Erişim deseni ne?** Anahtarla doğrudan erişim ve yapıya özgü işlemler (sayaç, sıralama, kuyruk) → Redis. Ad-hoc sorgu, join, filtreleme → veritabanı.
3. **Veri RAM'e sığıyor mu, ve buna değer mi?** Redis'te her şey RAM'de. 1 TB veri için 1 TB'tan fazla RAM gerekir (replikalarla 2 katı).

## 2. Problem → yapı haritası

| Problem | Redis yapısı | Atomiklik | Bu labda |
|---|---|---|---|
| Pahalı sorgunun sonucunu tut | String + TTL | `SET ... EX` | `cache/` |
| Sayaç, id üretimi | String + `INCR` | Tek komut | `atomic/` |
| "Sadece bir kişi yapsın" | String + `SET NX PX` | Tek komut / Lua | `lock/` |
| Rate limit | String / ZSET / Hash | Lua | `ratelimit/` |
| Aynı istek iki kez işlenmesin | String + `SET NX EX` | Tek komut | `idempotency/` |
| Stok, kontenjan, bilet | String + Hash | Lua | `inventory/` |
| Sıralama, ilk N, "ben kaçıncıyım" | ZSET | `ZINCRBY` | `leaderboard/` |
| Zamanı gelince çalışacak iş | ZSET (skor = zaman) + Hash | Lua | `delayedqueue/` |
| Kaybolmaması gereken olay akışı | Stream + consumer group | `XREADGROUP` / `XACK` | `streams/` |
| Bütün pod'lara anlık haber | Pub/Sub | — (ateşle-unut) | `pubsub/` |
| Oturum, çevrimiçi kullanıcılar | Hash + TTL / ZSET | Tek komut | `presence/` |
| Tekil sayım (milyonlarca) | HyperLogLog | `PFADD` | `probabilistic/` |
| Kullanıcı başına evet/hayır (aktif mi?) | Bitmap | `SETBIT` | `probabilistic/` |
| Yakındaki noktalar | Geo | `GEOADD` / `GEOSEARCH` | `probabilistic/` |
| Tek key'e aşırı yük | Parçalı key + yerel cache | — | `hotkey/` |
| "Bu eleman sette var mı?", az bellekle | Bloom filter (RedisBloom modülü) | — | Bu labda yok, modül gerektirir |
| Tam metin arama, vektör arama | RediSearch modülü | — | Bu labda yok. Genelde Elasticsearch / pgvector daha uygun |

## 3. Redis'i KULLANMA

| Durum | Neden | Bunun yerine |
|---|---|---|
| Paranın, siparişin tek kaynağı | Asenkron replikasyon + `everysec` fsync: yazma kaybolabilir | PostgreSQL, önünde Redis |
| Karmaşık sorgular, raporlar | Join, filtre, ad-hoc sorgu yok | Veritabanı, OLAP |
| Büyük, soğuk veri | RAM pahalı | Disk tabanlı DB, object storage |
| Uzun süre saklanacak, tekrar okunacak olaylar | RAM sınırlı, tek stream tek düğümde | Kafka |
| Doğruluk kilidi (iki kişi asla aynı anda) | Failover ve GC duraklamaları | DB satır kilidi, unique kısıt, fencing token |
| Büyük ikili dosyalar | Büyük key'ler her şeyi yavaşlatır | Object storage, Redis'te sadece referans |

## 4. Bir cluster mı, birden çok cluster mı?

**Cache ile durumu (state) aynı Redis'e koyma.** İkisi farklı ayarlar ister:

| | Cache cluster'ı | Durum cluster'ı (kilit, stok, kuyruk, oturum) |
|---|---|---|
| `maxmemory-policy` | `allkeys-lru` / `allkeys-lfu`: dolunca eskiyi sil | `noeviction`: dolunca yazmayı reddet, asla sessizce silme |
| Kalıcılık | Kapalı ya da sadece RDB | AOF `everysec` + RDB |
| Kaybın etkisi | DB'ye ek yük | İş mantığı bozulur |

İkisi aynı yerdeyse ve bellek dolarsa, `allkeys-lru` bir kilidi ya da bir stok sayacını "eski cache" sanıp silebilir. `noeviction` ise cache yazmalarını reddedip uygulamayı hata verdirir. Bu labın `k8s/11-redis-config.yaml` dosyası durum tuttuğu için `noeviction` kullanıyor.

### Eviction politikaları

| Politika | Ne yapar | Ne için |
|---|---|---|
| `noeviction` | Bellek dolunca yazma komutlarına hata döner | Durum, kuyruk |
| `allkeys-lru` | En uzun süredir kullanılmayan key'i siler | Genel cache |
| `allkeys-lfu` | En az sıklıkla kullanılanı siler | Popülerliği belirgin cache (birkaç sıcak, çok soğuk key) |
| `volatile-lru` / `volatile-ttl` | Sadece TTL'li key'ler arasından siler | Karışık kullanım, ama kırılgan: TTL'siz key'ler doldurursa yine hata |

## 5. Kalıcılık

| Seçenek | Nasıl | Kayıp penceresi | Bedel |
|---|---|---|---|
| Kapalı | Sadece RAM | Restart = her şey gider | Yok |
| RDB | Belli aralıklarla snapshot (`save 3600 1`) | Son snapshot'tan bu yana her şey | `fork()` sırasında bellek ve gecikme sıçraması |
| AOF `everysec` | Her yazma loglanır, saniyede bir fsync | ~1 sn | Disk G/Ç, AOF rewrite |
| AOF `always` | Her yazmada fsync | ~0 | Ciddi yavaşlama, nadiren değer |

Önemli: **kalıcılık failover kaybını çözmez.** AOF master'ın kendi diski için. Master ölüp replika devraldığında, replikaya ulaşmamış yazmalar AOF'ta olsa bile yeni master'da yoktur.

## 6. Boyutlandırma: kaba hesap

```
key başına bellek ≈ key adı + değer + ~50-90 byte yapı ek yükü
```

Örnek: 10 milyon oturum, her biri ~300 byte'lık bir Hash:

```
10.000.000 × (300 + ~100 ek yük)   ≈ 4 GB veri
+ fragmantasyon payı (~%20-30)     ≈ 5 GB
× 2 (her master'ın bir replikası)   ≈ 10 GB toplam RAM
+ fork payı (maxmemory'nin üstünde) → pod limiti maxmemory × ~1,3
```

Kendi verin için ölç: `MEMORY USAGE <key>` tek bir key'in gerçek boyutunu verir, `INFO memory` toplamı ve fragmantasyon oranını.

**Hız:** Tek bir Redis düğümü basit komutlarda saniyede ~100 bin işlem ve üstünü kaldırır. Darboğaz genelde ağ turu sayısıdır, Redis değil. 10 ayrı çağrı yerine tek bir Lua script'i ya da pipeline, hem doğruluk hem hız kazandırır.

## 7. Anti-pattern'ler

| Anti-pattern | Sorun | Doğrusu |
|---|---|---|
| `KEYS *` | Bütün key'leri tarar, o sürede Redis durur | `SCAN` (imleçli, parça parça) |
| Büyük key (1 milyon elemanlı liste, 50 MB'lık string) | Okuması, silmesi ve cluster'da taşınması herkesi bekletir | Parçala. `DEL` yerine `UNLINK` (arka planda siler) |
| TTL'siz cache | Bellek sonsuza kadar dolar | Her cache key'ine TTL + jitter |
| Herkes aynı anda düşen TTL | Cache avalanche | TTL'ye rastgele sapma (`cache/` senaryosu) |
| `GET` + `SET` ile koşullu güncelleme | Yarış durumu | Tek komut ya da Lua (`docs/01`) |
| Script içinde key adı üretmek | Cluster yanlış düğüme yönlendirir | Her key'i `KEYS[]` ile ver |
| Her şeyi tek hash tag'e koymak (`{app}`) | Bütün veri tek düğümde, cluster işe yaramaz | En küçük atomik birimi tag yap |
| Redis kilidiyle para korumak | Failover'da iki sahip | DB garantisi + fencing |
| Pub/Sub ile kaybolmaması gereken olay | Dinlemeyen kaçırır | Streams ya da Kafka |
| Uygulama saatiyle zaman kararı | Pod saatleri farklı | Lua içinde `redis.call('TIME')` |
| Her istekte yeni bağlantı | TCP + AUTH maliyeti | Uzun ömürlü bağlantı (Lettuce tek bağlantıyı çoklar) |

## 8. İşletme kontrol listesi

**İzlenecek metrikler** (`INFO` ve redis_exporter ile Prometheus):
- `used_memory` / `maxmemory` oranı, `mem_fragmentation_ratio`
- `evicted_keys` (durum cluster'ında 0 olmalı), `expired_keys`
- `instantaneous_ops_per_sec`, `connected_clients`, `blocked_clients`
- `keyspace_hits` / `keyspace_misses` (cache isabet oranı)
- Replika gecikmesi: `master_repl_offset` ile replikanın offset'i arasındaki fark
- `cluster_state`, `cluster_slots_fail`

**Teşhis komutları:**

| Komut | Ne gösterir |
|---|---|
| `SLOWLOG GET 10` | Eşiği aşan yavaş komutlar |
| `LATENCY DOCTOR` | Gecikme sıçramalarının sebebi (fork, disk) |
| `redis-cli --bigkeys` | En büyük key'ler (SCAN ile, güvenli) |
| `redis-cli --hotkeys` | En sık erişilen key'ler (LFU politikası gerekir) |
| `MEMORY USAGE key` | Tek key'in gerçek boyutu |
| `CLIENT LIST` | Kim bağlı, ne yapıyor |
| `CLUSTER NODES` / `CLUSTER INFO` | Topoloji ve sağlık |

**Güvenlik:**
- Parola ya da ACL kullanıcıları (`ACL SETUSER`): uygulamaya sadece ihtiyacı olan komutlar.
- Ağ: Redis'i asla internete açma. NetworkPolicy / security group.
- TLS: ağ güvenilir değilse (`tls-port`).
- Tehlikeli komutları kapat: `rename-command FLUSHALL ""`, `KEYS ""`, `CONFIG ""`.

## 9. Kendini test et

Bu soruları dokümanlara bakmadan cevaplayabiliyorsan, Redis tasarım toplantısında soru işareti kalmaz.

1. `GET` + `SET` ile sayaç artırmak neden yanlış? Doğrusu ne? Koşullu artış için ne gerekir?
2. `MULTI`/`EXEC` ile "stok yetiyorsa düş" yapılabilir mi? Neden?
3. Lua script'inde key'leri neden `KEYS[]` ile vermek zorundasın?
4. Kilidi bırakırken neden önce token'ı kontrol ediyoruz, ve bu neden tek adımda olmalı?
5. Redis kilidi alan bir pod GC'de 10 saniye donarsa ne olur? Fencing token bunu nasıl çözer?
6. Cache stampede, avalanche ve penetration nedir? Her birinin savunması ne?
7. Güncellemede cache'e yeni değeri yazmak yerine neden siliyoruz?
8. Sabit pencere, kayan pencere ve token bucket arasındaki fark? Hangisini ne zaman seçersin?
9. Gecikmeli kuyrukta alınan iş neden silinmeyip "processing"e taşınıyor?
10. Pub/Sub ile Streams arasındaki fark? Hangisi ne için?
11. 10 milyon kullanıcının bugün aktif olup olmadığını en az bellekle nasıl tutarsın?
12. Hash tag nedir, ne zaman kullanılır, ne zaman zarar verir?
13. Bir master öldü. Ne kadar sürede ne olur, hangi yazmalar kaybolabilir, istemci tarafında ne ayarlanmalı?
14. Cache ile kilitleri neden aynı Redis'e koymamalısın?
15. Tek bir key'e saniyede 200 bin okuma geliyor. Cluster'a düğüm eklemek çözer mi? Ne yaparsın?

Cevapların hepsi bu labın kodunda, testlerinde ve üç dokümanında.
