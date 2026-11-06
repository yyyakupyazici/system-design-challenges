# 01 · Atomiklik: Redis neden "doğru" çalışır, ve nerede çalışmaz

Bu labdaki her senaryonun özü aynı soru: **"oku → karar ver → yaz" işini, araya başka bir istemci girmeden nasıl yaparım?** Bu doküman Redis'in bu soruya verdiği beş cevabı, ne zaman hangisinin seçileceğini ve en sık yapılan hataları anlatır.

## 1. Temel: Redis komutları tek thread'de, sırayla çalışır

Redis komutları tek bir thread'de, birbiri ardına çalıştırır. Ağ G/Ç'si Redis 6'dan beri çok thread'li olabilir, ama komutların kendisi hâlâ tek thread'de yürür. Sonuç:

- **Tek bir komut her zaman atomiktir.** `INCR`, `SET NX`, `ZADD`, `HINCRBY`, `LPUSH`... Bir komut çalışırken başka hiçbir komut araya giremez.
- **İki komut arasına ise her şey girebilir.** `GET` ile `SET` arasında başka bir istemcinin `SET`'i çalışabilir.

Bunun bir bedeli de var: **uzun süren tek bir komut, herkesi bekletir.** `KEYS *`, 1 milyon elemanlı bir `SMEMBERS` ya da yavaş bir Lua script'i, o süre boyunca bütün istemcileri durdurur.

## 2. Problem: oku-değiştir-yaz yarışı

```
İstemci A: GET sayac → 10
İstemci B: GET sayac → 10
İstemci A: SET sayac 11
İstemci B: SET sayac 11      ← A'nın artışı kayboldu (lost update)
```

`AtomicCounterIT.readModifyWriteLosesAnUpdateWhenTwoClientsInterleave` bu adımları elle sıraya dizer ve sonucun 12 değil 11 olduğunu gösterir. Bu yarış sadece sayaçlarda değil, **"önce kontrol et, sonra yaz"** kalıbının olduğu her yerde vardır:

| Kalıp | Yarış senaryosu |
|---|---|
| Stok yetiyorsa düş | İki alıcı son ürünü aynı anda görür, ikisi de alır (eksi stok) |
| Kilit bende mi? Bendeyse sil | Kontrolden sonra kilidin süresi dolar, başkası alır, ben onun kilidini silerim |
| Limit altındaysa artır | 50 kişilik kampanyaya 53 kişi girer |
| Key yoksa yaz | İki pod aynı işi başlatır |

## 3. Beş çözüm, en basitten en esnek olana

### 3.1 Tek bir atomik komut (önce bunu ara)

Çoğu yarışın tek komutluk bir karşılığı vardır:

| İhtiyaç | Yanlış (iki çağrı) | Doğru (tek komut) |
|---|---|---|
| Sayaç | `GET` + `SET` | `INCR` / `INCRBY` |
| "Yoksa yaz" | `EXISTS` + `SET` | `SET key val NX` |
| "Yoksa yaz, süreli" | `SET NX` + `EXPIRE` | `SET key val NX PX 30000` |
| Puan ekle | `ZSCORE` + `ZADD` | `ZINCRBY` |
| Al ve sil | `GET` + `DEL` | `GETDEL` |
| Kuyruktan güvenli al | `RPOP` + `LPUSH` | `LMOVE` |

**Kural:** İki komut yazmadan önce, ikisini birden yapan tek bir komut var mı diye bak. Redis'in komut listesi bu yarışları kapatmak için tasarlanmıştır.

### 3.2 Lua script'i (koşul gerektiğinde)

Tek komut yetmiyorsa (bir koşul, bir hesaplama, birden çok key), işi bir Lua script'i olarak gönderirsin. **Script başından sonuna araya hiçbir komut girmeden çalışır.**

```lua
-- incr_if_below.lua: "limit altındaysa artır"
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
if v >= tonumber(ARGV[1]) then return 0 end
redis.call('INCR', KEYS[1])
return 1
```

Bu labda 14 script var, hepsi `app/src/main/resources/scripts/` altında ve her birinin başında neden script olması gerektiği yazıyor.

Lua'nın kuralları:
- **Dokunduğun her key'i `KEYS[]` ile ver.** Cluster, script'i hangi düğüme göndereceğini `KEYS`'e bakarak seçer. Key'i script içinde string olarak üretirsen (`'stock:' .. ARGV[1]`) cluster bunu bilemez ve script yanlış düğümde çalışabilir.
- **Cluster'da bütün `KEYS` aynı slotta olmalı** (hash tag: `stock:{sku}` ve `stock:{sku}:orders`). Değilse `CROSSSLOT` hatası alırsın. `ClusterIT` bunu gerçek bir cluster'da gösteriyor.
- **Kısa tut.** Script çalışırken Redis başka hiçbir şey yapmaz. 5 ms süren bir script, saniyede 200'den fazla çağrılamaz ve o sürede herkesi bekletir. `lua-time-limit` (varsayılan 5 sn) aşılırsa Redis yeni komutlara `BUSY` döner ve sadece `SCRIPT KILL` kabul eder.
- **`EVALSHA` kullan.** Spring'in `DefaultRedisScript`'i önce 40 karakterlik SHA'yı gönderir, script sunucuda yoksa gövdeyi gönderir. Her çağrıda script metnini göndermezsin.
- **Script'te geri alma (rollback) yok.** Script ortada hata verirse o ana kadar yaptığı yazmalar **kalır**. Bu yüzden önce bütün kontrolleri yap, sonra yaz (bu labdaki script'lerin hepsi böyle yazıldı).
- **Saat için `redis.call('TIME')`.** Kararı uygulamanın saatine göre verirsen, saatleri birkaç ms farklı olan pod'lar farklı karar verir. Rate limit ve kuyruk script'leri saati Redis'ten alıyor.

Redis 7 ile gelen **Functions** (`FUNCTION LOAD`, `FCALL`), Lua script'lerinin sunucuda kalıcı ve isimli halidir. Mantık aynı. Script'leri uygulamayla birlikte dağıtmak daha basit olduğu için bu lab `EVALSHA` kullanıyor.

### 3.3 MULTI / EXEC (transaction)

```
MULTI
DECRBY stok 1
HSET siparisler o-1 1
EXEC
```

`MULTI` ile `EXEC` arasındaki komutlar kuyruğa alınır ve `EXEC` anında hep birlikte, araya hiçbir şey girmeden çalışır. Ama iki önemli eksiği var:

1. **Koşul kuramazsın.** Komutlar kuyruğa alındığı için, ortadaki bir komutun sonucuna göre "o zaman şunu yapma" diyemezsin. `MULTI` "stok yetiyorsa düş" problemini tek başına çözmez.
2. **Rollback yok.** Bir komut çalışırken hata verirse (yanlış tipte key gibi), diğerleri yine çalışır.

**Spring + cluster notu:** Spring Data Redis, Lettuce cluster bağlantısında `MULTI`'yi desteklemez. Cluster'da çok-komutlu atomiklik için pratikte Lua kullanılır. Bu lab da bu yüzden hiçbir yerde `MULTI` kullanmıyor.

### 3.4 WATCH (iyimser kilit)

```
WATCH stok
GET stok              → 5
MULTI
DECRBY stok 1
EXEC                  → stok bu arada değiştiyse nil döner, baştan dene
```

`WATCH`'ladığın key `EXEC`'e kadar başkası tarafından değiştirilirse `EXEC` hiçbir şey yapmaz. Sen de baştan denersin. Bu, veritabanlarındaki `version` sütunlu iyimser kilitlemenin (optimistic locking) Redis karşılığı.

- Çakışma azsa iyi çalışır.
- **Çakışma çoksa kötüdür:** flash sale'de 1.000 kişi aynı stok key'ini `WATCH`'larsa 999'u başarısız olur, tekrar dener, yine başarısız olur. Lua'da ise herkes sırayla, tek seferde sonuç alır.
- `MULTI` gibi cluster'da Spring ile kullanılamaz.

**Sonuç:** Lua, `WATCH`'ın yaptığı her şeyi daha az ağ turuyla ve tekrar denemesiz yapar. `WATCH`'ı bilmek önemli, çünkü mülakatlarda sorulur ve eski kodlarda karşına çıkar.

### 3.5 Pipelining (atomik DEĞİL)

Pipelining birden çok komutu tek bir ağ turunda gönderir. **Hızlıdır ama atomik değildir.** Komutlar arasına başka istemcilerin komutları girebilir. Sık yapılan hata: "pipeline'a koydum, demek ki birlikte çalışır" sanmak. Pipelining performans aracıdır, doğruluk aracı değildir.

## 4. Karar tablosu

| Yöntem | Atomik mi? | Koşul kurulabilir mi? | Cluster'da (Spring) | Ne zaman |
|---|---|---|---|---|
| Tek komut | ✓ | Komutun kendi seçenekleriyle (`NX`, `XX`, `GT`) | ✓ | Her zaman önce bunu ara |
| **Lua** | ✓ | ✓ | ✓ (key'ler aynı slotta) | Koşul, birden çok key, hesaplama |
| MULTI/EXEC | ✓ | ✗ | ✗ | Koşulsuz toplu yazma (standalone) |
| WATCH + MULTI | ✓ (iyimser) | ✓ (tekrar deneyerek) | ✗ | Az çakışma, eski kod |
| Pipelining | ✗ | ✗ | ✓ | Sadece performans |

## 5. Bu labda hangi senaryo hangi yöntemi kullanıyor?

| Senaryo | Atomiklik ihtiyacı | Yöntem | Dosya |
|---|---|---|---|
| Sayaç | Artışlar kaybolmasın | `INCR` | `atomic/CounterService` |
| Koşullu sayaç | Limiti aşmasın | Lua | `incr_if_below.lua` |
| Cache yükleme kilidi | Tek pod yüklesin | `SET NX PX` | `cache/CacheAsideService` |
| Dağıtık kilit | Al + fencing token / sahibine özel bırak | Lua | `lock_*.lua` |
| Fencing | Eski token yazamasın | Lua | `fenced_write.lua` |
| Rate limit | Say + süre koy / pencereyi temizle + say + ekle | Lua | `rl_*.lua` |
| Idempotency | İlk gelen alsın | `SET NX EX` | `idempotency/IdempotencyService` |
| Flash sale stok | Yetiyorsa düş + siparişi kaydet | Lua | `stock_reserve.lua` |
| Liderlik tablosu | Puanlar kaybolmasın | `ZINCRBY` | `leaderboard/LeaderboardService` |
| Gecikmeli kuyruk | İki worker aynı işi almasın | Lua | `dq_claim.lua` |
| Stream | Her mesaj grupta tek tüketiciye | `XREADGROUP` (sunucu garantisi) | `streams/OrderEventStream` |

## 6. Atomiklik her şeyi çözmez: Redis'in garanti etmedikleri

Atomiklik **tek bir Redis düğümü içinde** geçerlidir. Şunları garanti etmez:

- **Kalıcılık.** `appendfsync everysec` ile son ~1 saniyelik yazma kaybolabilir.
- **Failover sonrası tutarlılık.** Replikasyon asenkrondur. Master bir yazmayı onaylayıp replikaya göndermeden ölürse, yeni master o yazmayı hiç görmemiştir. Kilit, stok düşümü ya da idempotency kaydı bu şekilde "geri alınabilir". `WAIT 1 100` ile yazmanın en az bir replikaya ulaşmasını bekleyebilirsin, ama bu da tam garanti değildir (bkz. [02](02-cluster-ve-dagitik-redis.md)).
- **Redis ile veritabanı arasında atomiklik.** "Redis'te stok düştüm, DB'ye sipariş yazamadım" durumu için telafi (release) ve mutabakat gerekir. Para gibi kritik veride gerçeğin kaynağı her zaman veritabanıdır, Redis önündeki hızlı kapıdır.
