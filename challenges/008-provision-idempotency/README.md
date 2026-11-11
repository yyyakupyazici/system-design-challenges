# #008 — Provizyon Servisi ve Gerçek Idempotency

**Kategori:** Klasik · **Zorluk:** ⭐⭐⭐⭐ Orta-İleri · **Tarih:** 2026-11-11

Fiat ve kripto varlıklara işlem öncesi provizyon (hold) koyan servis. Odak noktası **idempotency**. Bu servis için idempotency tek bir header'dan ibaret değil, dört katmanda çözülmesi gereken bir problem: API, iş kuralı, mesajlaşma ve dış dünya. PostgreSQL üzerinde, 1 milyon müşteri ölçeğinde.

---

## 1. Senaryo

Paramix kripto borsasında müşteriler al, sat, yatır ve çek yapıyor. Fiat (TRY, USD, EUR) ve kripto (BTC, ETH, USDT) varlıklar tutuluyor. Her para hareketinden önce ilgili tutar **provizyona** alınıyor:

- **Alım emri:** 1.000 TRY bloke edilir. Emir 980 TRY'ye dolarsa 980 TRY harcanır, 20 TRY geri döner. İptal edilirse 1.000 TRY'nin tamamı geri döner. Kimse kapatmazsa süre dolunca kendiliğinden serbest kalır.
- **Kripto çekim:** 120,5 USDT bloke edilir. Zincir onaylarsa harcanır, reddederse geri döner.

Eski sistemde yaşananlar:
- Mobil uygulama zaman aşımına uğrayınca isteği tekrar gönderdi ve aynı emir için iki provizyon açıldı. Müşterinin parası iki kez bloke edildi.
- Bir pod, çekimi zincire yayınladıktan hemen sonra öldü. Kurtarma job'ı işlemi "gönderilmemiş" sanıp tekrar yayınladı ve **çift çekim** oldu.
- Redis'te `SETNX` ile idempotency yapılıyordu. Redis failover'ında key'ler kayboldu ve aynı istek ikinci kez işlendi.
- Bakiye "önce oku, sonra yaz" ile düşülüyordu. Paralel iki emir aynı parayı iki kez bloke etti ve bakiye eksiye düştü.

## 2. Gereksinimler

| Metrik | Hedef |
|---|---|
| Müşteri / günlük aktif | 1.000.000 / ~150.000 |
| Provizyon işlemi (ortalama / tepe) | ~35 / ~1.500 işlem/sn (volatil piyasada 30-40 kat sıçrama) |
| Idempotency kaydı | ~3-5 milyon/gün, ~1 KB → 72 saat saklama ≈ 15 GB |
| Çift işlem / kayıp işlem / eksi bakiye | **Sıfır / sıfır / sıfır** |
| Provizyon API p99 | < 50 ms |
| Çekim | İstek anında 202; zincir onayı asenkron |
| Veritabanı | PostgreSQL 16 (tek primary + replikalar) |

## 3. Tartışmadan çıkanlar

| Konu | İlk fikir | Düzeltme |
|---|---|---|
| Idempotency kaydı nerede? | Hızlı olsun diye Redis | **Para ile aynı PostgreSQL transaction'ında.** Redis ve PG ayrı yerlerde olursa aradaki her çökme ya "para gitti, kayıt yok" ya da "kayıt var, para gitmedi" durumunu bırakır. Redis failover'ında da key'ler kaybolabilir |
| Eşzamanlı aynı key | `IN_PROGRESS` + `locked_until` lease | Tek transaction'lık işte buna **gerek yok**. Satır commit'e kadar başkasına görünmez; ikinci istek unique indekste bekler. Sahibi çökerse ROLLBACK olur ve kilit kendiliğinden kalkar. Takılı kalmış `IN_PROGRESS` oluşamaz. Lease yalnızca çok adımlı dış çağrılarda gerekir, o yüzden worker'a taşındı (§5.5) |
| Aynı key, farklı içerik | Eski cevabı dön | **422.** "100 USDT çek" key'iyle gelen "10.000 USDT çek" isteğine "tamam" demek en tehlikeli hata olurdu |
| Key kapsamı | Global unique | `(customer_id, key)`. Global olursa bir müşteri başkasının key'ini tahmin edip cevabını görebilir |
| Kayıtları silmek | Günlük `DELETE ... WHERE created_at < ...` | Partition + `DROP`. Ama partition'lı tabloda unique kısıt partition anahtarını içermek zorunda, bu yüzden `created_at` ile bölünemez. **Çözüm: UUIDv7 key** (§5.2) |
| Çekimi API'de bitirmek | İstek içinde zincire yayınla | İstek yolu dış dünyaya dokunmaz: provizyon + PENDING, sonra **202**. Zincir işini lease/fencing'li worker yapar |
| Çekim hata verdi | Provizyonu bırak (iade) | Para zincirde olabilir. **MANUAL_REVIEW**, provizyon bloke kalır. Otomatik iade, belirsiz durumda müşteriye bedava para demek |

## 4. Mimari

Diyagramlar: [mimari](design/architecture.mmd) · [idempotent istek akışı](design/idempotent-request.mmd) · [provizyon durumları](design/provision-states.mmd) · [çekim durumları](design/withdrawal-states.mmd)

```
İstek yolu (senkron, tek tx):   istemci → gateway → provision-service → PostgreSQL → cevap
Çekim (asenkron, adım adım):    withdrawals (PENDING) → worker: imzala → yayınla → onay → capture/release
Olaylar (en az bir kez):        outbox → relay → Kafka → tüketici (event_id ile inbox)
Denetim:                        mutabakat (bakiye = provizyonlar = ledger)
```

| Katman | Tekrar nereden gelir? | Koruma | Kod |
|---|---|---|---|
| **1. API** | Mobil retry, çift tık, gateway retry | `Idempotency-Key` (UUIDv7) + `idempotency_keys` + parmak izi | `idempotency/IdempotentExecutor` |
| **2. İş** | Aynı emir farklı key ile (key kaybı, emir servisinin retry'ı) | `UNIQUE (customer_id, operation_type, operation_ref)` + koşullu durum geçişleri | `domain/ProvisionService` |
| **3. Mesajlaşma** | Relay çöktü, Kafka tekrar teslim etti | Outbox (aynı tx) + `event_id` (tüketici inbox'ı) | `outbox/*` |
| **4. Dış dünya** | Yayından sonra çökme, timeout | İmzalı tx yayından ÖNCE kalıcı, önce hash ile sorgu, sonra aynı baytlar + fencing | `withdrawal/WithdrawalWorker` |
| **Yatırım** | Webhook + zincir dinleyici + mutabakat aynı olayı getirir | Doğal anahtar `UNIQUE (source, external_ref)` | `domain/DepositService` |

Pod'lar **durumsuz**. Kilit, kira ve liderlik bilgisinin hepsi PostgreSQL'de. Aynı key'in iki kopyası iki farklı pod'a düşse de işlem bir kez yapılır (demo §9'da iki pod ile gösteriliyor).

## 5. Önerilen tasarım, konu konu

### 5.1 API idempotency: tek transaction, üç adım

```
BEGIN
  ① KAP     INSERT INTO idempotency_keys ... ON CONFLICT DO NOTHING
            └─ kapılamadı → kaydı oku → parmak izi farklı? 422 : kayıtlı cevabı bayt bayt dön
  ② İŞ      SAVEPOINT · provizyon + bakiye + ledger + outbox
            └─ iş hatası (bakiye yetersiz…) → ROLLBACK TO SAVEPOINT; hata da bir sonuçtur
  ③ SONUÇ   UPDATE idempotency_keys SET response_status, response_body
COMMIT
```

- **Eşzamanlı kopyalar:** İkinci isteğin INSERT'ü, unique indeksteki commit edilmemiş satır yüzünden bekler. İlki commit ederse ikincisi `DO NOTHING` alır ve kayıtlı cevabı döner. İlki rollback olursa ikincisinin INSERT'ü başarılı olur ve işi o yapar. Yani veritabanı aynı zamanda bir dağıtık kilit görevi görüyor.
- **Bekleme süresi sınırlı:** Kilit beklemesi `SET LOCAL lock_timeout` ile sınırlanıyor ve ayar **sadece bu INSERT** için açılıp hemen kapatılıyor. Süre aşılırsa istemci `409 + Retry-After: 1` alıyor. Böylece bekleyen istekler bağlantı havuzunu tüketmiyor ve bakiye satırı gibi başka kilit beklemeleri yanlışlıkla "istek sürüyor" diye raporlanmıyor. Hata tespiti Spring'in istisna sınıfına değil SQLSTATE `55P03`'e bakıyor; Spring bu kodu sürümden sürüme farklı sınıflara eşliyor.
- **Neden savepoint:** İş hatası, komutun yarım bıraktığı yazmaları geri almalı ama key kaydını korumalı. Beklenmeyen hatalar ise (DB koptu, bug) yakalanmıyor; bütün transaction geri alınıyor, kayıt kalmıyor ve aynı key ile tekrar denemek güvenli oluyor.
- **İş hatası kaydedilir:** "Bakiye yetersiz" de bir sonuç. Aynı key ile tekrar sorulduğunda aynen döner, müşteri arada para yatırmış olsa bile. Key o denemeyi temsil ediyor; yeni deneme yeni key ister. Stripe ve benzeri API'ler de böyle çalışıyor.
- **Bayt bayt aynı cevap:** `response_body` kolonu `TEXT`, `JSONB` değil. JSONB anahtar sırasını ve boşlukları normalize ettiği için tekrar oynatılan cevap ilkinden farklı olurdu.
- **Parmak izi:** SHA-256, ham gövdeden değil **ayrıştırılmış ve normalize edilmiş komuttan** alınıyor. `100`, `100.00` ve alan sırası farkı aynı izi verir; tutar, varlık ya da hedef farkı farklı iz verir. İşlem adı (`provision.create`, `withdrawal.create`) da ize giriyor, böylece bir işlemin key'i başka bir işlemde kullanılamıyor.

### 5.2 Key politikası ve partition'lar

Key **UUIDv7** olmalı (RFC 9562): ilk 48 biti, key'in üretildiği milisaniyeyi taşır.

- **Kabul penceresi:** Key 24 saatten eskiyse ya da 5 dakikadan fazla ileri tarihliyse istek reddediliyor. Kaydı silinmiş olabilecek bir key, sunucu tarafından "ilk kez görüyorum" diye işlenirse aynı işlem iki kez yapılır. Bunun tek dürüst çözümü, böyle key'leri hiç kabul etmemek.
- **Saklama ≥ pencere:** Bu kural `retention ≥ accept-window + skew + 1h` olarak uygulama açılışında zorlanıyor. Ayar yanlışsa uygulama hiç başlamıyor.
- **Partition:** PostgreSQL'de partition'lı tablodaki PK, partition anahtarını içermek zorunda. Tablo `created_at`'e göre bölünseydi, gece yarısını geçen bir retry diğer güne düşer ve tekilleştirme kırılırdı. Burada `key_day` **key'in içindeki zamandan** türetiliyor: aynı key her zaman aynı güne, dolayısıyla aynı partition'a düşüyor. Eski günler `DROP TABLE` ile atılıyor; bloat, vacuum yükü ve WAL patlaması olmuyor.
- **Bakım:** Partition'lar 3 gün önceden açılıyor. Saati ileri bir telefon gece yarısına doğru "yarın"ın partition'ına yazabilir. Bakım fonksiyonları advisory lock ile her pod'dan güvenle çağrılabiliyor.

### 5.3 Provizyon: durum makinesi ve atomik bakiye

- **Bakiye tek UPDATE ile düşülüyor:** `UPDATE balances SET available = available - x, held = held + x WHERE ... AND available >= x`. Kontrol ve düşüm bölünmüyor, satır kilidi eşzamanlı istekleri sıraya sokuyor. Son savunma hattı `CHECK (available >= 0)`.
- **Durum geçişleri koşullu:** `UPDATE provisions SET status = 'CAPTURED' WHERE id = ? AND status = 'HELD' RETURNING ...`. 10 capture ve 10 release aynı anda gelse de tam biri kazanıyor (test bunu doğruluyor).
- **Kısmi capture:** `captured ≤ amount`, kalan kısım aynı transaction'da `available`'a dönüyor.
- **Her geçiş dört şey yazar:** provizyon, bakiye, ledger journal'ı ve outbox olayı. Hepsi aynı transaction'da.
- **Süre:** Alım emri provizyonları süreli. Süpürücü `FOR UPDATE SKIP LOCKED` ile çalıştığı için o anda capture edilen satırı beklemiyor, atlıyor. Çekim provizyonları **süresiz** ve bu bir DB `CHECK` kısıtı. Para zincirdeyken süre dolup bakiyenin geri gelmesi felaket olurdu.
- **Başkasının provizyonu:** 404 dönüyor, 403 değil. Provizyonun var olduğu bile dışarı sızmıyor.
- **Tutar:** `NUMERIC(38,18)` ve varlık ölçeği (TRY 2, BTC 8, ETH 18). Ölçekten hassas tutarlar **reddediliyor**, yuvarlanmıyor. API'de tutarlar string olarak taşınıyor; JS'de 18 basamaklı ETH tutarı `double`'a çevrilirse bozulur.

### 5.4 İş seviyesinde idempotency

`UNIQUE (customer_id, operation_type, operation_ref)`. Aynı emir için ikinci provizyon, **farklı bir key ile** gelse bile açılmıyor; `409 PROVISION_ALREADY_EXISTS` döner ve mevcut provizyonun id'si iletilir. Key kaybolsa, uygulama yeniden kurulsa ya da emir servisi kendi retry'ını yapsa bile tek emir tek provizyon demek. Provizyon satırı bakiyeden **önce** yazılıyor; çakışmayı kaybeden istek bakiyeye hiç dokunmuyor.

### 5.5 Çekim: lease, fencing, çökme sonrası kurtarma

İstek yolu tek transaction: provizyon + `PENDING` çekim + olay, ardından `202`. Sonrası worker'ın işi:

```
PENDING ──① imzala, tx_hash + imzalı baytları YAZ──▶ SIGNED ──② hash ile sor, yoksa yayınla──▶ BROADCAST ──③ onay?
                                                                                    CONFIRMED → capture │ FAILED → release
```

| Çökme anı | Neden güvenli |
|---|---|
| ① imzadan sonra, yazmadan önce | Hiçbir şey yayınlanmadı. Devralan worker yeniden imzalar |
| ② yayından sonra, `BROADCAST` yazılmadan önce | Durum `SIGNED` kalır. Devralan **önce hash ile zincire sorar**, işlem oradaysa yayınlamaz. Sorgu yanılsa bile aynı imzalı baytları yollar; zincir aynı nonce'u ikinci kez kabul etmez. İki kat koruma var |
| ③ onaydan sonra, capture'dan önce | `CONFIRMED` + capture tek transaction. Devralan tekrar sorar ve capture eder |

- **Kira (lease):** `claim_token` + `lease_until`, iş `FOR UPDATE SKIP LOCKED` ile alınıyor. N worker çalışıyor ve hiçbiri diğerini beklemiyor. Kirası dolmuş satırı başka bir worker devralabiliyor. Zaman her yerde DB'nin `now()`'ı; pod saatleri kayabilir.
- **Fencing:** Her yazma `WHERE claim_token = :benim` koşuluyla yapılıyor. 40 saniyelik bir GC duraklamasından uyanan eski worker 0 satır günceller ve `LostLeaseException` ile hiçbir şey yazmadan çekilir. DB'yi fencing, zinciri ise aynı imzalı işlem koruyor; ikisi birlikte gerekli.
- **MANUAL_REVIEW:** Üst üste N hatadan sonra çekim insana bırakılıyor ve provizyon **bloke kalıyor**. Otomatik iade sadece zincirin kesin `FAILED` cevabında yapılıyor.

### 5.6 Yatırım: doğal anahtar

Yatırımda Idempotency-Key kullanılmıyor. Banka EFT referansı ya da `zincir:tx_hash:log_index` zaten dünyada tek. `UNIQUE (source, external_ref)`: aynı olay üç kaynaktan gelse de bakiyeye bir kez yazılıyor (`201` yeni, `200` zaten vardı). Aynı referans farklı tutar veya müşteri ile gelirse `409`. Bu ya bir entegrasyon hatası ya da saldırıdır; sessizce yutulmuyor.

### 5.7 Outbox

Olay, iş verisiyle aynı transaction'da yazılıyor. Relay her turda `pg_try_advisory_xact_lock` alıyor; böylece pod sayısından bağımsız olarak tek relay çalışıyor ve aynı provizyonun olayları sırayla gidiyor. Teslim garantisi "en az bir kez"; tüketici `event_id` ile kendi inbox'ında tekilleştiriyor. Yayınlanan satırlar **id listesiyle** siliniyor. `DELETE ... WHERE id <= son` yanlış olurdu: identity değeri INSERT anında alınıyor, commit sırası farklı olabiliyor ve küçük id'li ama henüz commit edilmemiş bir olay yayınlanmadan silinebilirdi.

### 5.8 Ledger ve mutabakat

- **Ledger:** Çift kayıt tutuluyor; her journal'ın toplamı sıfır. Kovalar: `AVAILABLE`, `HELD`, `EXTERNAL` (dış dünyadan gelen), `SETTLED` (capture ile çıkan). `UPDATE` ve `DELETE` trigger ile yasak; düzeltme ters kayıtla yapılıyor. Sistem hesapları için `balances` satırı tutulmuyor, aksi halde bütün işlemlerin kilitlediği tek bir sıcak satır (hot row) oluşurdu.
- **Mutabakat:** `REPEATABLE READ` ile tek bir snapshot üzerinde üç kontrol yapılıyor: `held` = HELD provizyonların toplamı, bakiye = ledger, journal toplamı = 0. Kısıtlar ve idempotency hataları **önler**; mutabakat önlenemeyeni **yakalar**. Testlerde her senaryodan sonra çalışıyor.

### 5.9 Hata sözleşmesi (problem+json)

| HTTP | `code` | Kaydedilir mi? | İstemci ne yapmalı |
|---|---|---|---|
| 400 | `IDEMPOTENCY_KEY_MISSING` / `_INVALID` / `_EXPIRED` | Hayır | Yeni UUIDv7 üret |
| 409 | `IDEMPOTENCY_REQUEST_IN_PROGRESS` + `Retry-After` | Hayır | **Aynı key** ile bekleyip tekrar dene |
| 422 | `IDEMPOTENCY_KEY_REUSED` | Hayır | Bug: yeni istek için yeni key |
| 422 | `INSUFFICIENT_FUNDS`, `INVALID_AMOUNT`, `CAPTURE_EXCEEDS_HOLD` | **Evet** | Yeni deneme = yeni key |
| 409 | `PROVISION_ALREADY_EXISTS`, `PROVISION_NOT_HELD` | **Evet** | `context` içindeki id/durumu kullan |
| 503 | `TEMPORARILY_UNAVAILABLE` | Hayır (rollback) | **Aynı key** ile tekrar dene |

Kural: 5xx ve 409-in-progress durumunda **aynı key** ile tekrar dene. 4xx iş hatasında yeni deneme için yeni key kullan.

## 6. Trade-off'lar ve alternatifler

| Seçim | Alternatif | Neden bu |
|---|---|---|
| Idempotency PG'de, aynı tx | Redis `SETNX` | Doğruluk tek kaynakta. Ek maliyet işlem başına iki satır yazma; tepe 1.500 tx/sn'de PG için sorun değil |
| Kilit = unique indeks beklemesi | `IN_PROGRESS` + lease | Sahibi ölünce kilit kendiliğinden kalkıyor, takılı kayıt oluşmuyor. Bedeli: bekleyen istek bir DB bağlantısı tutuyor (`lock_timeout` ile sınırlı) |
| UUIDv7 zorunlu | Serbest key + `created_at` partition | Pencere ve partition ancak key'in kendi zamanı ile doğru çalışıyor. Bedeli: istemci kütüphanesi UUIDv7 üretmeli (tek satır) |
| Çekim 202 + worker | Senkron 200 | Dış çağrı DB transaction'ını tutmuyor, kurtarma tek yerde toplanıyor. Bedeli: istemci durumu GET ile izliyor ya da olay dinliyor |
| JdbcTemplate, açık SQL | JPA/Hibernate | Koşullu UPDATE, `ON CONFLICT`, `SKIP LOCKED`, `RETURNING` bu servisin özü; ORM bunları saklar ya da zorlaştırır |
| Bakiye satırı (müşteri × varlık) | Bakiyeyi ledger'dan anlık toplamak | Toplama okuma maliyetini artırır. Satır hızlı; ledger ise gerçeğin kaynağı ve mutabakat ikisini eşitliyor |

## 7. Ölçek ×10 olursa (10M müşteri, ~15.000 tx/sn tepe)

- **Yazma:** Tek primary sınıra yaklaşır. `customer_id` ile **shard** gerekir (Citus ya da uygulama seviyesinde N cluster). Tasarım buna hazır: bir işlemin dokunduğu bütün satırlar (key, provizyon, bakiye, ledger) aynı müşteriye ait, yani shard'lar arası transaction yok.
- **Sıcak müşteri:** Market maker'lar gibi çok sık işlem yapan hesaplarda aynı `(customer, asset)` satırı kilitleniyor. Bir satırda saniyede ~500-1.000 işlem sınırı var. Çözüm, bakiyeyi alt hesaplara (sub-balance) bölmek ya da bu hesaplar için toplu (batch) provizyon.
- **Bağlantı:** PgBouncer (transaction mode). `SET LOCAL` kullanıldığı için transaction pooling ile uyumlu, bilerek `SET` değil `SET LOCAL` seçildi.
- **Ledger:** Aylık partition ve soğuk veri için arşiv. Mutabakat replika üzerinde artımlı çalışır.
- **Idempotency tablosu:** Günlük partition zaten yatay ölçekleniyor. Gerekirse `retention` kısaltılıp `accept-window` buna göre ayarlanır (açılış kontrolü birlikte değiştirmeye zorluyor).

## 8. Testler

`mvn verify`. Gerçek PostgreSQL (Testcontainers), gerçek HTTP, gerçek eşzamanlılık. Her testten sonra mutabakat çalışır ve tek bir fark testi kırar.

| Test | Kanıtladığı |
|---|---|
| `sameKeyConcurrentlyExecutesExactlyOnce` | Aynı key ile 100 eşzamanlı istek → 1 provizyon, 1 journal, 100 bayt bayt aynı cevap |
| `sameKeyDifferentPayloadIsRejected` | Key + farklı tutar → 422, hiçbir şey değişmez |
| `semanticallyEqualRequestIsReplayed` | `100` ↔ `100.00`, alan sırası, büyük/küçük harf → aynı istek |
| `keyIsScopedPerCustomer` / `keyCannotBeReusedAcrossOperations` | Key kapsamı müşteri ve işlem |
| `invalidKeysAreRejectedBeforeAnyWork` | Eksik, v4, 25 saatlik, gelecekten gelen key |
| `businessErrorIsStoredAndReplayed` | Yetersiz bakiye kaydedilir; savepoint yarım provizyonu geri alır |
| `inFlightRequestGets409AndCrashedOwnerLeavesNoTrace` | Sahip sürerken 409 + Retry-After; sahip çökerse iz kalmaz, retry işi yapar |
| `concurrentHoldsNeverOverdraw` | 1.000 TRY'ye 50 × 30 TRY eşzamanlı: tam 33 geçer, bakiye 10 |
| `sameOperationRefWithDifferentKeyConflicts` | İş seviyesinde idempotency |
| `concurrentCaptureAndReleaseExactlyOneWins` | 10 capture + 10 release yarışında tek kazanan |
| `partialCaptureReturnsRemainder`, `expiredProvisionIsReleased`, `captureCannotExceedHold` | Durum makinesi |
| `depositIsDeduplicatedByExternalReference`, `ledgerIsAppendOnly` | Doğal anahtar, değiştirilemez defter |
| `crashAfterBroadcastDoesNotDoubleSpend` | Yayından sonra süreç ölür → devralan worker tekrar yayınlamaz |
| `staleWorkerIsFenced` | İki worker aynı durumu görüyor; kirası devralınmış olan hiçbir şey yazamıyor |
| `repeatedFailuresGoToManualReviewAndKeepFundsHeld` | Belirsizlikte otomatik iade yok |
| `withdrawalProvisionIsOwnedByWithdrawal` | Çekim provizyonu süresiz (DB kısıtı), API'den kapatılamaz |
| `partitionsAreCreatedAheadAndDroppedAfterRetention` | Partition bakımı idempotent |
| `IdempotencyUnitTest` | UUIDv7, pencere, gece yarısı partition günü, parmak izi, saklama ayarı kontrolü |

**Testlerin testi (mutasyon):** Üç kritik satır tek tek bozuldu ve ilgili testlerin kırıldığı doğrulandı:
- Fencing token kontrolü kaldırılınca `staleWorkerIsFenced` kırılıyor.
- Parmak izi karşılaştırması kaldırılınca iki test kırılıyor.
- Savepoint kaldırılınca `businessErrorIsStoredAndReplayed` kırılıyor.

> Docker Engine 29+ kullanılıyorsa Testcontainers 1.21.4+ gerekir. 1.21.3 API 1.32 ile konuşuyor ve "client version 1.32 is too old" hatası alıyor. `pom.xml` bu yüzden sürümü sabitliyor.

## 9. Nasıl çalıştırılır

```bash
cd challenges/008-provision-idempotency/app && mvn verify
```

```bash
cd challenges/008-provision-idempotency && docker compose up --build
```

```bash
./scripts/demo.sh
```

Demo turu: tekrarlanan yatırım bildirimi; **iki farklı pod'a** aynı key ile 20 paralel istek (tek provizyon); aynı key farklı tutar (422); aynı emir yeni key (409); kısmi capture ve tekrarı; kayıtlı "bakiye yetersiz"; kripto çekimin PENDING → BROADCAST → CONFIRMED akışı; mutabakat.

Elle deneme:

```bash
KEY=$(python3 -c 'import os,time;ms=int(time.time()*1000);r=os.urandom(10);b=ms.to_bytes(6,"big")+bytes([0x70|r[0]&15,r[1],0x80|r[2]&63])+r[3:];h=b.hex();print(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")')
curl -i -X POST localhost:8081/v1/provisions -H 'Content-Type: application/json' \
  -H 'X-Customer-Id: 42' -H "Idempotency-Key: $KEY" \
  -d '{"asset":"TRY","amount":"100","operationType":"TRADE","operationRef":"order-1"}'
```

| Uç | Açıklama |
|---|---|
| `POST /v1/provisions` | Provizyon aç (`Idempotency-Key`). `ttlSeconds` opsiyonel (varsayılan 15 dk, en fazla 24 saat) |
| `POST /v1/provisions/{id}/capture` | `{"amount": "980"}` ya da boş gövde = tamamı |
| `POST /v1/provisions/{id}/release` | Serbest bırak |
| `GET /v1/provisions/{id}`, `GET /v1/balances` | Okuma |
| `POST /v1/withdrawals`, `GET /v1/withdrawals/{id}` | Çekim (202) ve durumu |
| `POST /internal/deposits` | Yatırım bildirimi (doğal anahtar) |
| `GET /internal/reconciliation` | Mutabakat farkları (boş liste = tutarlı) |
| `GET /actuator/prometheus` | `idempotency_requests_total{operation,outcome}`, `provisions_total`, `withdrawals_*` |

## 10. Bilerek dışarıda bırakılanlar

- **Kimlik:** `X-Customer-Id` header'ını demoda istemci gönderiyor. Üretimde gateway JWT'den yazar, istemcinin göndermesine izin verilmez.
- **Kafka adaptörü:** `EventPublisher` arayüzü hazır, demo loglara yazıyor. Kafka için key = `aggregateId` olur ve producer `enable.idempotence=true` ile çalışır (bkz. 007).
- **Gerçek zincir:** `ChainGateway` sözleşmesi hazır (imzala, yayınla, durum). Sıcak cüzdan nonce yönetimi, HSM/MPC imzalama ve reorg sonrası onay derinliği bu challenge'ın kapsamı dışında.
- **Emir eşleştirme:** Capture'ı çağıran emir motoru ayrı bir sistem. Burada capture sadece bir API çağrısı.
