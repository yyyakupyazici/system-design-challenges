# #007 — Gerçek Zamanlı Mesajlaşma ve Presence

**Kategori:** Klasik · **Zorluk:** ⭐⭐⭐⭐⭐ İleri · **Tarih:** 2026-11-07

WebSocket gateway'leri, Redis'te yönlendirme tablosu ve Pub/Sub, sohbet başına sıra numarası, idempotent gönderim, outbox + Kafka ile fan-out, çevrimdışı senkron, presence ve sohbet içinde para isteği.

---

## 1. Senaryo

Paramix uygulamasına sohbet ekleniyor:
- Birebir ve grup mesajları (grup en fazla 500 kişi).
- Sohbet içinde **para isteği**. Ödeme 006'daki cüzdandan geçiyor.
- Destek sohbeti.
- 2 milyon aboneli, tek yönlü **kampanya kanalı**.

Tek sunuculu deneme sürümünde yaşananlar:
- Sunucu yeniden başlayınca bütün bağlantılar aynı anda geri bağlanıp sunucuyu tekrar çökertti.
- Kötü mobil ağda bazı mesajlar iki kez, bazıları hiç gitmedi.
- Grupta mesajlar herkeste farklı sırada göründü.
- Ödenmiş para isteği sohbette "bekliyor" kaldı, kullanıcı tekrar ödedi.

## 2. Gereksinimler

| Metrik | Hedef |
|---|---|
| Kullanıcı / günlük aktif | 18 milyon / 3 milyon |
| Eşzamanlı WebSocket (tepe) | 1,2 milyon |
| Mesaj (ortalama / tepe) | ~1.400 / 35.000 mesaj/sn |
| Teslimat (tepe) | ~100.000/sn |
| Çevrimiçi teslim | p99 < 500 ms |
| Kayıp / görünen mükerrer | Sıfır / sıfır |
| Saklama | Mesaj 1 yıl, para isteği 5 yıl |
| Gateway pod'u | ≤ 50.000 bağlantı → en az 25 pod |

## 3. Tartışmadan çıkanlar

| Konu | Senin cevabın | Düzeltme / tamamlama |
|---|---|---|
| "Kim hangi gateway'de?" | Cihaz bazında bir tablo, kalıcı olsun | Tablo doğru ama **geçici** olmalı: bağlantıların çoğu temiz kapanmaz (tünel, şarj, pod çökmesi). Kalıcılık mesajda olmalı, konumda değil |
| 40.000 yenileme/sn | Gateway'deki herkesi tek komutla güncelle | Ağ turunu azaltır ama Redis'in işini azaltmaz (cluster'da `CROSSSLOT`, dev komut Redis'i kilitler). Doğrusu: kullanıcı kayıtları süresiz, **süre sadece 25 gateway kaydında** → 5 yazma/sn |
| Gateway'ler arası taşıma | Kafka (kaybolmamalı) | Kalıcılık zaten DB'de ve istemci imlecinde (uçtan uca ilkesi). Gateway'ler arası **Redis Pub/Sub** yeterli. Kafka'nın yeri: DB'ye yazılmış olayı fan-out, push, cüzdan gibi tüketicilere taşımak |
| Sıra | Gönderen telefonda UTC timestamp | Telefon saatine güvenilmez, timestamp aynı ms'de çakışır ve kaçırılan mesaj fark edilemez. Doğrusu: **sunucunun verdiği, sohbet başına, boşluksuz seq** |
| Mükerrer mesaj | Mesaj servisinde idempotency key ile kaydet | Doğru yer. "Önce bak, sonra yaz" yarışır, bu yüzden kararı **unique kısıt** versin. Alıcı tarafta da istemci `(sohbet, seq)` ile tekilleştirir |

## 4. Mimari

Diyagramlar: [mimari](design/architecture.mmd) · [üç yol (kalıcı, hızlı, kurtarma)](design/delivery-paths.mmd) · [para isteği durumları](design/payment-request-states.mmd)

```
Kalıcı yol:    istemci → gateway → mesaj servisi → DB (seq + mesaj + outbox, tek transaction) → ✓
Hızlı yol:     DB → relay → Kafka → fan-out → rehber → Redis Pub/Sub (gw:<id>) → gateway → istemci
Kurtarma yolu: istemci → SYNC {imleç} → DB'den imleçten sonrası
```

**Kural: önce kaydet, sonra teslim et.** Gönderen ✓'yı mesaj veritabanına yazılınca alır. Hızlı yol kaybederse (gateway öldü, soket koptu, Pub/Sub'ı dinleyen yoktu) mesaj kaybolmaz, gecikir: istemci yeniden bağlanınca ya da sırada boşluk görünce imleciyle ister.

| Bileşen | İşi | Durum |
|---|---|---|
| **Gateway** | Soketleri tutar, çerçeveyi servise verir, cevabı yazar. İş mantığı yok | Sadece kendi soketleri (yerel defter) |
| **Mesaj servisi** | Doğrular, seq verir, kaydeder, outbox'a yazar | Yok |
| **PostgreSQL** | Mesajlar, imleçler, para istekleri, outbox | **Gerçeğin kaynağı** |
| **Outbox relay** | Outbox → Kafka. Advisory lock ile tek lider | Yok |
| **Kafka** `chat.events` | Key = sohbet id: bir sohbetin olayları sırayla | Log |
| **Fan-out worker** | Üyeler → rehber → canlı gateway'ler → gateway başına tek PUBLISH. Çevrimdışılara push | Yok |
| **Redis** | Rehber `conn:{user}`, gateway lease `gw:alive`, son görülme, Pub/Sub | Geçici |

## 5. Önerilen tasarım, konu konu

### 5.1 Bağlantı ve yönlendirme (zorluk #1)

- **Yerel defter** (`LocalSessions`): "bu cihaz bende, şu sokette." Sadece gateway'in belleğinde.
- **Ortak rehber** (`RoutingTable`): `HASH conn:{userId}` → `cihaz: gateway-instance`. Bağlanınca `HSET`. Kopunca `HDEL`, ama Lua ile **sadece kayıt hâlâ bu gateway'i gösteriyorsa** (cihaz bu arada başka gateway'e bağlanmış olabilir).
- **Gateway lease** (`LiveGateways`): `ZSET gw:alive`, gateway 5 sn'de bir skorunu yeniler. 15 sn yenilenmeyen gateway ölü sayılır ve bütün kayıtları birden geçersiz olur. Ölü kayıtları kimse silmez, okuyan yok sayar (tembel temizlik). Canlı listesi her saniye bir kez belleğe alınır.
- **Instance kimliği** `gw-7#a81f`: aynı adla yeniden doğan pod'un eski kayıtlarla karışmaması için (fencing mantığı).
- **Pub/Sub telsizi** (`GatewayInbox`): her gateway açılışta kendi kanalına (`gw:<instance>`) abone olur. Fan-out, alıcının gateway'ine `{targets, frame}` yayınlar, aynı gateway'deki 40 alıcı için tek PUBLISH.
- **Gateway içi sıra:** Pub/Sub mesajları tek thread'de alınır, sonra **cihaz başına şeride** (16 şerit) verilir. Spring'in varsayılanı her mesajı ayrı thread'de işliyordu ve testte aynı cihaza giden mesajların sırası bozuldu. Bu, outbox'taki "partition'a göre sıralı paralellik" fikrinin gateway içindeki hali.
- **Yavaş istemci:** soket `ConcurrentWebSocketSessionDecorator` ile sarılı. Gönderim 5 sn'yi ya da 512 KB tamponu aşarsa bağlantı kapanır (backpressure). Yavaş bir telefon gateway'in belleğini doldurmaz, senkronla toparlar.

### 5.2 Geri bağlanma fırtınası (zorluk #2)

| Durum | Savunma |
|---|---|
| **Planlı kapanış** (rolling deploy, ölçek küçültme) | `GatewayDrainer`: readiness kapanır, her istemciye `RECONNECT {afterMs: 0–20 sn rastgele}` gider. 50.000 istemci aynı saniyede değil, 20 saniyeye yayılarak (~2.500/sn) başka gateway'lere bağlanır. Kayıtlar ve lease silinir |
| **Çökme** | `AdmissionLimiter`: gateway saniyede en fazla N el sıkışma kabul eder, fazlası `503 + Retry-After: 1–10 sn rastgele` alır. İstemci de üstel geri çekilme + jitter uygular |
| **Senkron yükü** | Aynı anda en fazla 32 senkron sorgusu (bulkhead). Sığmayan `BUSY` alır ve tekrar dener |
| Kubernetes | `maxUnavailable: 2`, `terminationGracePeriodSeconds: 60`, `preStop` ile önce endpoint'ten düş |

### 5.3 Veri modeli ve DB seçimi

```sql
conversations (id, kind DIRECT|GROUP|BROADCAST, last_seq)          -- sohbet başına sayaç
conversation_members (conversation_id, user_id, role, read_seq)    -- okuma imleci kullanıcı başına
messages (conversation_id, seq, sender_id, client_msg_id, kind, body, payment_request_id, server_time)
    PRIMARY KEY (conversation_id, seq)
    UNIQUE (conversation_id, client_msg_id)                         -- idempotency
device_cursors (user_id, device_id, conversation_id, delivered_seq) -- yeni bağlanan cihaz buradan senkronlar
payment_requests (id, conversation_id, requester_id, payer_id, amount_kurus, status, wallet_transfer_id, expires_at)
outbox (id, conversation_id, payload)
```

**Erişim deseni:** Neredeyse her sorgu `conversation_id` ile başlar ("bu sohbetin seq > 41 mesajları"). Bu yüzden **partition/shard key = conversation_id.**

**Hacim:** Günde 120 milyon mesaj × ~300 byte ≈ 36 GB/gün, yılda ~13 TB. Bu kod tek bir PostgreSQL kullanıyor. Üretim için iki seçenek:

| | PostgreSQL, sohbet id'sine göre shard'lı (ör. Citus) + aylık partition | Cassandra / ScyllaDB |
|---|---|---|
| Seq üretimi | Sohbet satırı kilidi, mesajla aynı transaction | Hafif transaction (LWT) ya da tek yazar (sohbeti bir partition sahibine bağlamak) |
| Idempotency | Unique kısıt | `INSERT ... IF NOT EXISTS` |
| Saklama | `DROP PARTITION` (anında) | TTL |
| Ne zaman | Ekip PostgreSQL biliyor, para isteği gibi ilişkisel veri var | Yazma ağırlıklı, çok büyük hacim |

Kodda saklama bir gece job'ı (`RetentionJob`): 1 yıldan eski mesajları 5.000'lik parçalarla siler. Para isteğine bağlı mesajlar 5 yıl kalır.

### 5.4 Sıra ve tekilleştirme (zorluk #3, #4)

`MessageService.send`, tek transaction'da:

```sql
UPDATE conversations SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq;   -- satır kilidi: sohbete yazanlar sıraya girer
INSERT INTO messages (...) ON CONFLICT (conversation_id, client_msg_id) DO NOTHING;
   -- çakışma: aynı mesaj tekrar geldi → ROLLBACK (numara boşa gitmez) → mevcut mesajı aynı seq ile dön, PUBLISH etme
INSERT INTO outbox (...);
```

- **Sırayı sunucu verir**, mesajın sunucuya geliş sırasıyla. "Gerçek" sıra yok, çünkü telefonlar arasında ortak saat yok. Önemli olan **herkesin aynı sırayı** görmesi.
- **Sohbet başına sayaç:** yük milyonlarca sohbete dağılır. Tek global sayaç sıcak key olurdu.
- **Neden Redis `INCR` değil:** failover'da son numaralar kaybolup tekrar verilebilir. `INCR` başarılı, DB yazması başarısız olursa sohbette kalıcı boşluk kalır.
- **Tekilleştirme iki uçta:** gönderen tarafta unique kısıt, alıcı tarafta istemci `(sohbet, seq)` ile. Ortadaki katmanlar mesajı iki kez taşıyabilir, sorun değil.

### 5.5 Fan-out (zorluk #5)

| Sohbet | Strateji | Neden |
|---|---|---|
| Birebir, grup (≤ 500) | **Fan-out on write:** üyeler → canlı cihazlar → gateway başına tek PUBLISH. Çevrimdışı üyelere push | Alıcı sayısı küçük, gecikme önemli |
| Kampanya kanalı (2 milyon) | **Tek PUBLISH `bc`**: her gateway kendi yerel abonelerine dağıtır. Üye listesi dolaşılmaz, **push yok**, çevrimdışı abone uygulamayı açınca senkronla çeker (fan-out on read) | 2 milyon teslimat ya da 2 milyon push kaydı yazmak yerine 1 mesaj + 25 gateway |

Gateway, bağlanan kullanıcının abone olduğu kanalları bağlantı anında yerel dizine alır.

### 5.6 Presence (zorluk #6)

- **Ayrı bir heartbeat yok.** Çevrimiçi = canlı bir gateway'de en az bir cihazı var, yani yönlendirme tablosundan türetilir. Ek yazma sadece "son görülme": son cihaz koptuğunda bir kez.
- **Çekme modeli:** istemci ekrandaki kişileri sorar (`PRESENCE_QUERY`, en fazla 100). Değişikliği herkese itmek ünlü hesap problemini doğurur: 100.000 takipçili biri her bağlanıp koptuğunda 100.000 bildirim.
- **Gizlilik:** `presence_hidden` olan kullanıcı için ne çevrimiçi ne son görülme döner.
- **"Yazıyor..."** geçici sinyal: DB'ye yazılmaz, Kafka'dan geçmez, doğrudan Pub/Sub. Kişi başı 2 saniyede bir.

### 5.7 Çoklu cihaz ve çevrimdışı senkron (zorluk #7)

- Mesaj alıcının **bütün** canlı cihazlarına ve gönderenin **diğer** cihazlarına gider (gönderen cihaz hariç).
- **İletildi (✓✓):** cihaz `ACK` → cihaz imleci ilerler → birebir sohbette karşı tarafa makbuz.
- **Okundu:** `READ` → kullanıcının okuma imleci (bütün cihazlar için tek) → olay bütün üyelere ve okuyanın diğer cihazlarına gider: "telefonda okudum, web'de de okundu."
- **Senkron:** istemci imleçlerini gönderir. Yoksa sunucudaki cihaz imleci, o da yoksa (yeni cihaz) her sohbetin son 50 mesajı. Sayfa 200 mesaj, `hasMore` ile devam.

### 5.8 Para isteği (zorluk #8)

[Durum diyagramı](design/payment-request-states.mmd). Mesaj servisi ile cüzdan ayrı sistemler, aralarında dağıtık transaction yok. Tutarlılık **iki yönlü** bir bağlantıdan ve üç yoldan geliyor.

**Sohbet → cüzdan (senkron çağrı):**
1. **Kapma:** `PENDING → PROCESSING` koşullu `UPDATE`. İki cihazdan aynı anda "Öde"ye basılsa da sadece biri cüzdanı çağırır.
2. **Cüzdan:** `transfer(idempotencyKey = "payreq-<id>")`. Aynı anahtarla tekrar = aynı transfer.
3. **Kapanış:** `PROCESSING → PAID` ve sohbete **sistem mesajı** ("✅ 250,00 TL ödendi") **aynı transaction'da**.

Senkron, çünkü "Öde"ye basan kullanıcı sonucu birkaç yüz milisaniyede görmek istiyor.

**Cüzdan → sohbet (olay):** 006'daki cüzdan her transferi outbox ile `wallet.events` topic'ine yayınlıyor (fraud, muhasebe ve bildirim de dinliyor). Sohbet servisi bu topic'i kendi consumer group'unda (`chat-payments`) dinler:

| Olay | Sohbette ne olur |
|---|---|
| `TRANSFER_COMPLETED` + anahtar `payreq-<id>` | `PENDING`/`PROCESSING`/`EXPIRED`/`DECLINED` → `PAID` + sistem mesajı. **Sohbet dışından yapılan ödeme** (cüzdan ekranı, destek) ve süresi dolduktan sonra yapılan ödeme de yansır: para gittiyse sohbet bunu göstermeli |
| `TRANSFER_REVERSED` | `PAID → REVERSED` + "↩️ ödeme iade edildi" (chargeback, fraud iptali) |
| Tutar uyuşmuyor | Durum değiştirilmez, alarm metriği (`chat.payment.amount_mismatch`). Sessizce "ödendi" demek yanlış, insan bakmalı |
| Anahtar `payreq-` ile başlamıyor | Yok sayılır (para isteğinden gelmeyen bir transfer) |

**Üç yol birlikte:**

| Yol | Ne zaman işe yarar |
|---|---|
| **Hızlı yol** (senkron cevap) | Normal durum: basan cihaz sonucu hemen görür |
| **Olay yolu** (`wallet.events`) | Cevap kayboldu → saniyeler içinde kapanır. Sohbet dışından ödeme, sonradan iade |
| **Güvenlik ağı** (reconciler) | Olay da bir şekilde kayboldu → `PROCESSING`'de takılanları aynı anahtarla cüzdana tekrar sorar |

Üçü de aynı koşullu durum geçişlerinden geçer ve sistem mesajlarının `client_msg_id`'si deterministik (`payreq:<id>:PAID`). **Hangi yol önce gelirse o yazar, diğerleri etkisiz kalır**, sohbete aynı "ödendi" mesajı iki kez düşmez. Olay, senkron cevaptan önce de gelebilir (cüzdan yavaşsa). Bu da testte kanıtlandı. Bu, mesaj teslimindeki kalıcı / hızlı / kurtarma yollarının aynı mantığı.

### 5.9 Ağ topolojisi

- **L4 load balancer** (NLB): L7'de her WebSocket ingress pod'unda da bellek tutar.
- **Sticky session gerekmez:** hangi gateway'e düşülürse düşülsün, rehber ve Pub/Sub mesajı doğru gateway'e taşır.
- Uzun zaman aşımları (1 saat), tamponlama kapalı. İstemci 30 sn'de bir `PING` gönderir (ara cihazlar boşta bağlantıyı kesmesin).
- **HPA sinyali pod başına bağlantı sayısı** (`chat_gateway_connections`, hedef 40.000), CPU değil. Küçülme yavaş, çünkü küçülmek bağlantı koparmak demek.

### 5.10 Redis çökerse: "DB'ye yazıldı ama publish edilemedi"

Mesaj o anda iki yerde güvende: **veritabanında** ve **Kafka'da** (olay işlenmedi, offset commit edilmedi). Kayıp riski yok. Soru, kullanıcının ne zaman göreceği. İlk sürümde iki boşluk vardı:

- Spring Kafka'nın varsayılan hata işleyicisi 10 kez **beklemeden** dener ve olayı atlar. 1 sn'lik bir Redis kesintisi bile canlı teslimi kaybettiriyordu.
- Push, Redis işlemlerinden sonra yapıldığı için Redis çökünce push da duruyordu.

Dört katmanlı çözüm:

| Katman | Ne yapar | Hangi durumu kapatır |
|---|---|---|
| **Beklemeli tekrar deneme** (`KafkaConfig`) | 100 ms'den başlayıp ikiye katlanarak (en fazla 5 sn arayla) 30 sn boyunca tekrar dener | Kısa kesinti (failover ~10 sn): olay atlanmaz, canlı yoldan teslim edilir |
| **Push ayrı consumer group'ta** (`PushWorker`, `chat-push`) | Canlı teslimden bağımsız. Rehbere ulaşılamazsa **şüphede herkese** (gönderen hariç) bildirir | Redis çökünce çevrimdışı kullanıcılar bildirimsiz kalmaz |
| **Pub/Sub sondası + RESYNC** (`PubSubHealth`) | Gateway saniyede bir kendi kanalına sonda yayınlar. 3 sn dönmezse yolu bozuk sayar. Tekrar dönünce: ① bütün bağlantılarını rehbere **yeniden yazar** (Redis verisini kaybetmiş olabilir) ② istemcilere 0–10 sn'ye yayılmış `RESYNC` gönderir | Uzun kesinti ve **veri kaybıyla yeniden başlama**: açık bağlantısı olan kullanıcı kaçırdığını saniyeler içinde görür. Sonda, Redis'in yavaş aboneyi sessizce koparmasını (`client-output-buffer-limit`) da yakalar |
| **HEADS** (istemci) | Mesaj içeriği olmadan her sohbetin son seq'i. İstemci ön plana gelince, RESYNC alınca ve açıkken dakikada bir çağırır, sadece geride kaldığı sohbetleri SYNC eder | Diğer her şey atlansa bile gecikmenin üst sınırı ~1 dk |

Gateway, Redis yokken de bağlantı kabul eder ve mesaj gönderimi (DB yolu) çalışmaya devam eder. Rehber kaydı Redis gelince sondanın tetiklediği toparlanmada yazılır.

**Ders:** "Kayıp yok" ile "kullanıcı zamanında görüyor" aynı şey değil. İlkini kalıcı yol garanti eder. İkincisi için her bağımlılığın çökme senaryosunda **hangi başka işin de durduğunu** ve **kullanıcının kaçırdığını nasıl fark ettiğini** tek tek düşünmek gerekir.

### 5.11 Darboğazlar ve trade-off'lar

| Karar | Kazanılan | Verilen |
|---|---|---|
| Önce kaydet, sonra teslim et | Kayıp sıfır | ✓ gecikmesi = bir DB transaction'ı (~5–10 ms) |
| Seq DB'de, sohbet satırı kilidiyle | Boşluksuz, failover'da tekrar yok | Tek bir sohbetin yazma tavanı ≈ 1 / kilit süresi (yüzlerce/sn). Sohbet başına yeterli |
| Hızlı yolda Pub/Sub | Basit, hızlı | Kaybolabilir (senkron yakalar). Cluster'da klasik PUBLISH bütün düğümlere kopyalanır → SPUBLISH ya da ayrı Redis |
| Kampanya kanalında fan-out on read | 2 milyon yazma yerine 1 | Çevrimdışı aboneye push yok |
| Presence çekme modeli | Ünlü hesap fırtınası yok | Ekrandaki durum anlık değil, sorgu aralığı kadar eski |

**En çok korkutan üç nokta:**
- **Bölge çapında kesinti** sonrası 1,2 milyon geri bağlanma (admission limit ve jitter yumuşatır, ama kapasite planı şart).
- **Çok aktif tek bir grup** (sohbet satırı kilidinin tavanı).
- **Pub/Sub Redis'inin tek noktası.** Çökerse hızlı yol durur. Kayıp yok, push sürer, Redis gelince RESYNC ile herkes toparlanır (§5.10), ama RESYNC dalgası senkron yükünü sıçratır (bu yüzden 10 sn'ye yayılıyor ve senkron bulkhead'li).

## 6. Kod

`app/`: Spring Boot 3.5 / Java 21 / Spring WebSocket / PostgreSQL / Redis / Kafka. Tek imaj, roller env ile seçilir.

| Paket | İçerik |
|---|---|
| `gateway` | `ChatWebSocketHandler`, `WebSocketConfig` (el sıkışmada kabul kontrolü), `LocalSessions`, `RoutingTable`, `LiveGateways`, `GatewayInbox` (cihaz şeritleri), `PubSubHealth` (sonda + RESYNC), `GatewayDrainer`, `AdmissionLimiter` |
| `message` | `MessageService` (gönder, ACK, READ, senkron, geçmiş), repository'ler |
| `outbox` | `OutboxWriter`, `OutboxRelay` (advisory lock lideri), `RetentionJob` |
| `fanout` | `FanoutWorker` (Kafka), `DeliveryRouter`, `DeliveryPublisher`, `EphemeralRelay` (yazıyor) |
| `presence` | `PresenceService` |
| `payment` | `PaymentRequestService`, `WalletEventsConsumer` (cüzdan olayları), `PaymentJobs` (reconciler, süre dolumu), `SystemMessages`, `WalletClient` (+ test için `FakeWalletClient`) |
| `push` | `PushWorker` (ayrı consumer group), `PushSender` (üretimde 004'ün bildirim platformu) |
| `api` | REST: geçmiş, presence, demo kullanıcı/sohbet oluşturma |

### WebSocket protokolü

`ws://host/ws?userId=42&deviceId=phone` (kimlik demo amaçlı. Üretimde el sıkışmada JWT doğrulanır.)

| İstemci → sunucu | Sunucu → istemci |
|---|---|
| `SEND {clientMsgId, conversationId, body, kind?, amountKurus?, payerId?}` | `SENT {clientMsgId, seq, serverTime, duplicate}` (✓) |
| `ACK {conversationId, seq}` | `MSG {message}` |
| `READ {conversationId, seq}` | `RECEIPT {conversationId, userId, seq, receipt: DELIVERED\|READ}` |
| `SYNC {cursors: {sohbet: seq}}` | `SYNC_RESULT {conversations: [...]}` |
| `HEADS` | `HEADS_RESULT {conversations: [{conversationId, lastSeq, readSeq}]}` |
| `TYPING {conversationId}` | `TYPING {conversationId, userId}` |
| `PRESENCE_QUERY {userIds}` | `PRESENCE {users}` |
| `PAY {paymentRequestId}`, `DECLINE {...}` | `PAY_RESULT {status}` |
| `PING` | `PONG`, `RECONNECT {afterMs}` (gateway kapanıyor), `RESYNC {afterMs}` (hızlı yol kesildi, senkronla), `ERROR {code, message, ref}` |

### Çalıştırma

```bash
docker compose up --build        # PostgreSQL + Redis + Kafka + 2 gateway (8081, 8082) + worker
```

Kubernetes: `kubectl apply -k k8s/` (gateway ×25 + HPA + PDB, worker ×3, PostgreSQL, Redis, Kafka, Ingress, NetworkPolicy).

### Testler

```bash
cd app
export IT_PG_URL=jdbc:postgresql://localhost:5432/chat IT_PG_USER=postgres IT_REDIS_HOST=localhost IT_REDIS_PORT=6379
mvn verify      # Docker varsa env gerekmez (Testcontainers); Kafka her zaman gömülü
```

Entegrasyon testleri **iki ayrı Spring uygulaması** (gw-a ve gw-b) ayağa kaldırır. İkisi aynı PostgreSQL, Redis ve Kafka'ya bağlıdır, yani gerçek bir mini cluster. Drain testi üçüncü bir gateway açıp kapatır.

| Test | Kanıtladığı |
|---|---|
| `CrossGatewayDeliveryIT` | Ali gw-a'da, Ayşe gw-b'de: mesaj Pub/Sub ile geçer. ✓, ✓✓, okundu geri döner. Relay'lerden tam olarak biri lider. Yabancı ve hatalı çerçeveler hata alır, bağlantı kopmaz |
| `IdempotencyAndOrderingIT` | Aynı mesaj iki gateway'den üç kez gönderilir: tek satır, hepsine aynı seq, sayaç boşa artmaz, Ayşe bir kez görür. İki kişi farklı gateway'lerden aynı anda 30'ar mesaj atar: seq 1–60 boşluksuz, iki farklı gateway'deki üyeler aynı sırayı görür |
| `OfflineAndMultiDeviceIT` | Çevrimdışıya push, dönüşte imleçten senkron (yeni cihaz, kayıtlı cihaz imleci, istemci imleci). Ölü gateway'deki bayat kayıt yok sayılır. Bir cihazda okunan mesaj diğerinde de okundu olur |
| `PaymentRequestIT` | İki cihazdan aynı anda "Öde": para bir kez. Cüzdan cevabı kaybolunca reconciler aynı anahtarla tamamlar, para bir kez. Bakiye yetersiz → tekrar dene. Süresi dolan ödenemez. Yetki ve geçersiz tutar |
| `WalletEventsIT` | Test cüzdanın rolünü oynar. Sohbet dışından ödeme sohbete bir kez yansır (olay iki kez gelse de). Sonradan iade kartı "iade edildi" yapar. Cüzdan olayı senkron cevaptan **önce** gelir: olay yolu yazar, senkron yol mesajı tekrarlamaz, para bir kez gider. Tutarı uyuşmayan ve ilgisiz olaylar yok sayılır |
| `BroadcastPresenceIT` | Kampanya kanalı iki gateway'deki abonelere ulaşır, push yok, abone yazamaz, geç gelen senkronla alır. "Yazıyor" iletilir ama saklanmaz. Presence: son görülme ve gizlilik |
| `RedisOutageIT` | **A.** Redis 4 sn donduruluyor, mesaj bu sırada gönderiliyor: beklemeli tekrar deneme sayesinde canlı yoldan teslim edilir (tekrar deneme kapatılınca bu test başarısız oluyor, denendi). **B.** Redis kapatılıyor ve **boş** açılıyor: gönderme çalışır, çevrimdışına push gider, gateway sondası toparlanır, rehberi yeniden doldurur, RESYNC gönderir, istemci HEADS + SYNC ile kaçırdığını alır, sonraki mesaj yine canlı gelir |
| `GatewayDrainIT` | Boşaltılan gateway 30 istemciye 0–2 sn'ye yayılmış RECONNECT gönderir, rehberden siler, yeni bağlantıya 503 döner |
| `AdmissionLimiterTest` | 5.000 eşzamanlı el sıkışmadan sadece kova kadarı kabul edilir, Retry-After rastgele |

**Sonuç** (yerel PostgreSQL 16 + Redis 7, gömülü Kafka): 2 birim + 22 entegrasyon testi geçti, art arda çalıştırmalarda da. `RedisOutageIT` Redis'i yerel süreç olarak yönettiği için sadece `IT_REDIS_HOST=localhost` ve `redis-server` PATH'teyken çalışır; Testcontainers ile atlanır.

**Testlerin yakaladığı gerçek hatalar:**
- Spring Kafka'nın varsayılan hata işleyicisi kısa bir Redis kesintisinde olayları beklemeden atlıyordu (§5.10).
- Spring'in Redis dinleyicisi her Pub/Sub mesajını ayrı bir thread'de işliyordu ve gw-b'deki üye grup mesajlarını farklı sırada gördü. Cihaz şeritleriyle düzeltildi (§5.1).

## 7. Dürüst notlar

- `docker-compose.yml` ve `k8s/` bu ortamda çalıştırılmadı (Docker ve Kubernetes yok). Sadece YAML sözdizimi kontrol edildi.
- Kimlik doğrulama demo amaçlı (sorgu parametresi ve `X-User-Id`). Üretimde JWT.
- Cüzdan (006) ve push (004) arayüz olarak tanımlı. Testlerde sahte uygulamaları kullanılıyor. `wallet.events` topic'inin sahibi cüzdan servisi: sohbet servisi onu oluşturmaz, sadece dinler (testte gömülü Kafka'da önceden açılıyor).
- Tek PostgreSQL, tek Redis. Shard'lama, aylık partition ve Pub/Sub için ayrı Redis tasarımda anlatıldı (§5.3, §5.11), kodda yok.
- HPA'nın bağlantı sayısı metriği için Prometheus Adapter kurulumu gerekir.
