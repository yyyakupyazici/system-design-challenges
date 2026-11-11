# #007 — Gerçek Zamanlı Mesajlaşma ve Presence

**Kategori:** Klasik · **Zorluk:** ⭐⭐⭐⭐⭐ İleri · **Tarih:** 2026-11-07

WebSocket gateway'leri, Redis'te yönlendirme tablosu ve Pub/Sub, sohbet başına sıra numarası, idempotent gönderim, outbox + Kafka ile fan-out, çevrimdışı senkron, presence ve sohbet içinde para isteği.

---

## 1. Senaryo

Paramix uygulamasına sohbet ekleniyor:
- Birebir ve grup mesajları (grup en fazla 500 kişi).
- Sohbet içinde **para isteği**. Ödeme 006'daki cüzdandan geçiyor. Para isteği ayrı bir servis (`payment-request-service`), sohbet sadece kartını gösteriyor (§5.8).
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

Diyagramlar: [mimari](design/architecture.mmd) · [üç yol (kalıcı, hızlı, kurtarma)](design/delivery-paths.mmd) · [para isteği durumları](design/payment-request-states.mmd) · [para isteği: iki servis arası akış](design/payment-request-flow.mmd)

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
payment_cards (payment_request_id, conversation_id, requester_id, payer_id, amount_kurus, status, version)  -- salt okunur kart (§5.8)
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

### 5.8 Para isteği (zorluk #8): ayrı servis, sohbette sadece kart

[İki servis arası akış](design/payment-request-flow.mmd) · [durum diyagramı](design/payment-request-states.mmd)

**İlk sürümde** para isteği sohbet servisinin içindeydi: `SEND kind=PAYMENT_REQUEST` mesajla aynı transaction'da `payment_requests` satırı açıyor, `PAY` çerçevesi cüzdanı çağırıyordu. Çalışıyordu, ama yanlış yerdeydi:

| Soru | Sohbetin içindeyken | Neden sorun |
|---|---|---|
| Para isteği sadece sohbetten mi gelir? | Evet, `conversation_id NOT NULL` | Ödeme linki, fatura, uygulama içi "iste" ekranı da para isteği. Her biri sohbet servisine bağımlı olurdu |
| "Öde" butonu neye bağlı? | Gateway'in WebSocket'ine | Gateway drain/deploy sırasında para hareketi kesilir. Para akışı en az değişen, en çok denetlenen kod olmalı; sohbet en sık deploy edilen |
| Kim denetlenir? | Sohbet kodunun tamamı (para tablosuna yazabiliyor) | Ödeme denetimi (PCI, BDDK) bütün sohbet ekibini kapsar |
| Tutarlılık | Sohbet + para tek veritabanında, tek transaction | Kolay ama bağlar: biri ölçeklenirken diğeri de taşınmak zorunda |

**Bounded context:** para isteğinin kuralları (kim ödeyebilir, süre, idempotent ödeme, iade) ödeme alanına ait. Sohbet onun **bir görünümü** (kart). Doğru sınır:

```
payment-request-service  (SAHİBİ)                         sohbet (GÖRÜNÜM)
  HTTP: oluştur / öde / reddet / sorgula                    payment.events'i dinler
  kendi veritabanı: payment_requests + outbox                payment_cards (salt okunur kopya)
  durum makinesi, cüzdan, reconciler, süre dolumu           kart mesajı + "✅ ödendi" sistem mesajı
  wallet.events'i dinler (dışarıdan ödeme, iade)            para mantığı YOK
            │                                                  ▲
            └──── outbox → Kafka "payment.events" (key = istek id) ──┘
```

**Akış:**
1. Ali'nin uygulaması `POST /v1/payment-requests` (`Idempotency-Key`, `origin: {kind: CHAT, ref: <sohbet id>}`) → servis `PENDING` yazar ve aynı transaction'da `CREATED` olayı (outbox).
2. Sohbet olayı alır, iki kişinin de sohbetin üyesi olduğunu kontrol eder, kartı ve kart mesajını yazar (gönderen = Ali). Hızlı yol iki tarafa da `MSG` iletir.
3. Ayşe kartta "Öde"ye basar → uygulama `POST /v1/payment-requests/{id}/pay` (sohbete DEĞİL). Kapma (`PENDING → PROCESSING`, koşullu `UPDATE`) → cüzdan `transfer("payreq-<id>")` → `PAID` + `PAID` olayı (tek tx).
4. Sohbet `PAID` olayını alır → kartı günceller + "✅ 250,00 TL ödendi". Ayşe'nin uygulaması sonucu HTTP cevabından zaten biliyor; diğer cihazlar ve Ali olaydan öğrenir.

**Ödeme tarafının üç yolu** (servis içinde, değişmedi): hızlı yol (cüzdanın senkron cevabı), olay yolu (`wallet.events`: cevap kayboldu, sohbet dışından ödeme, sonradan iade), güvenlik ağı (reconciler `PROCESSING`'de takılanları aynı anahtarla tekrar sorar). Hepsi aynı koşullu geçişlerden geçer: hangisi önce gelirse o yazar, olay bir kez yayınlanır.

| `wallet.events` olayı | Para isteği |
|---|---|
| `TRANSFER_COMPLETED` + anahtar `payreq-<id>` | `PENDING`/`PROCESSING`/`EXPIRED`/`DECLINED` → `PAID`. Para gittiyse istek "ödendi" olmalı |
| `TRANSFER_REVERSED` | `PAID → REVERSED` (chargeback, fraud iptali) |
| Tutar uyuşmuyor | Durum değişmez, alarm (`payreq.amount_mismatch`). İnsan bakmalı |
| Anahtar `payreq-` ile başlamıyor | Yok sayılır |

**Olay sözleşmesi (`payment.events`):** her olay isteğin **tam anlık görüntüsü + sürüm numarası**:

```json
{"type":"PAID","paymentRequestId":31,"version":3,"status":"PAID","requesterId":7,"payerId":9,
 "amountKurus":25000,"description":"Akşam yemeği","originKind":"CHAT","originRef":"81","reason":null}
```

- **Sürüm:** her durum geçişinde `version = version + 1` (`PROCESSING` gibi olay üretmeyen geçişler de artırır, bu yüzden sürümler 1, 3, 4 gibi atlayabilir). Sohbet kartı `UPDATE ... WHERE payment_cards.version < yeni` ile günceller: **tekrar gelen ya da eski olay hiçbir şey değiştirmez**, tek SQL, yarışsız.
- **Anlık görüntü:** olay sadece "PAID oldu" demiyor, kartı kurmak için gereken her şeyi taşıyor. "Ödendi" olayı "oluşturuldu"dan önce gelse bile (tüketici yeniden dengelendi, eski olay geç kaldı) sohbet kartı doğru kurar; geç gelen `CREATED` (sürüm 1) yok sayılır. Kafka key = istek id olduğu için aynı isteğin olayları normalde sıralı gelir; sıra karışıklığına dayanıklılık ek güvence.
- **Bağımsız sözleşme:** sohbet servisi olay sınıfını kendisi tanımlar (`paymentcard.PaymentRequestEvent`), ödeme servisinin kodunu import etmez. Bilinmeyen alanlar yok sayılır: ödeme servisi alan ekleyince sohbet bozulmaz.

**Sohbet tarafı (`PaymentCardsConsumer`, consumer group `chat-payment-cards`):** tek transaction'da kart + mesaj + outbox.

| Durum | Ne olur |
|---|---|
| Kart yok | Kartı yaz + kart mesajı (`PAYMENT_REQUEST`, gönderen = isteyen). Olay `CREATED` değilse durum mesajı da |
| Daha yeni sürüm | Kartı güncelle + durum mesajı ("✅ ödendi", "❌ reddedildi", "⌛ süresi doldu", "↩️ iade edildi") |
| Eski sürüm / tekrar | Hiçbir şey |
| `origin` CHAT değil | Yok sayılır (ödeme linki, fatura) |
| İsteyen ya da ödeyen sohbetin üyesi değil | **Kart yazılmaz.** `originRef`'i isteyen kişi yazıyor; kontrol olmasa biri yabancı bir sohbete kart düşürebilirdi |

Mesajların `client_msg_id`'si deterministik (`payreq:<id>:CARD`, `payreq:<id>:PAID`): aynı olay iki kez işlense de mesaj bir kez yazılır. Kart mesajını sonradan senkronla çeken cihaz onu **güncel** durumla görür (mesaj `payment_cards`'a join'li okunur).

**Bedeli:** iki servis arasında artık tek transaction yok. Kart, ödemeden birkaç yüz ms sonra güncellenir (outbox relay + Kafka). Bu kabul edilebilir çünkü "Öde"ye basan kişi sonucu HTTP cevabından anında alıyor; gecikme sadece diğer cihazların kartında. Karşılığında: para akışı sohbetin deploy'undan bağımsız, aynı servis linkten ve faturadan gelen istekleri de karşılıyor, ve ödeme verisine sadece ödeme servisi erişiyor (k8s `NetworkPolicy` ile ağ seviyesinde de).

**Mimar dersi:** "Bu veri kime ait?" sorusunu, verinin **nerede göründüğüne** bakarak değil, **kuralları kimin koyduğuna** bakarak cevapla. Kart sohbette görünür ama kuralları ödemenin. Görünüm, sahibin olaylarından kurulan bir kopyadır (read model); kopyanın tutarlılığı sürüm numarası ve idempotent yazmayla sağlanır, dağıtık transaction'la değil.

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

İki Maven modülü, üst `pom.xml` ile birlikte derlenir:

| Modül | Ne | Veritabanı | Dış arayüz |
|---|---|---|---|
| `payment-request-service/` | Para isteğinin sahibi | `payreq` (kendi) | HTTP `/v1/payment-requests`, yayınlar: `payment.events`, dinler: `wallet.events` |
| `app/` | Sohbet: gateway + worker (tek imaj, roller env ile) | `chat` | WebSocket `/ws`, REST, dinler: `payment.events` |

Spring Boot 3.5 / Java 21 / Spring WebSocket / PostgreSQL / Redis / Kafka. `app` ödeme modülünü sadece **test** bağımlılığı olarak görür (uçtan uca test için); üretim kodu import etmez.

**`app` paketleri:**

| Paket | İçerik |
|---|---|
| `gateway` | `ChatWebSocketHandler`, `WebSocketConfig` (el sıkışmada kabul kontrolü), `LocalSessions`, `RoutingTable`, `LiveGateways`, `GatewayInbox` (cihaz şeritleri), `PubSubHealth` (sonda + RESYNC), `GatewayDrainer`, `AdmissionLimiter` |
| `message` | `MessageService` (gönder, ACK, READ, senkron, geçmiş), `MessagePoster` (sunucu adına mesaj: kart, durum), repository'ler |
| `outbox` | `OutboxWriter`, `OutboxRelay` (advisory lock lideri), `RetentionJob` |
| `fanout` | `FanoutWorker` (Kafka), `DeliveryRouter`, `DeliveryPublisher`, `EphemeralRelay` (yazıyor) |
| `presence` | `PresenceService` |
| `paymentcard` | `PaymentCardsConsumer` (payment.events → kart + mesaj), `PaymentCardRepository` (sürümlü upsert), `PaymentRequestEvent` (sözleşme) |
| `push` | `PushWorker` (ayrı consumer group), `PushSender` (üretimde 004'ün bildirim platformu) |
| `api` | REST: geçmiş, presence, demo kullanıcı/sohbet oluşturma |

**`payment-request-service` paketleri:**

| Paket | İçerik |
|---|---|
| `api` | `PaymentRequestController`: `POST /v1/payment-requests` (Idempotency-Key), `POST /{id}/pay`, `POST /{id}/decline`, `GET /{id}`, `GET /pending` |
| `domain` | `PaymentRequestService` (durum makinesi; her geçiş + olay tek tx), `PaymentRequestRepository` (koşullu geçişler, `version + 1`), `PaymentJobs` (reconciler, süre dolumu) |
| `outbox` | `Outbox` (olay yazma, `MANDATORY` tx), `OutboxRelay` (advisory lock lideri → `payment.events`) |
| `wallet` | `WalletClient` (+ `FakeWalletClient`), `WalletEventsConsumer` (`wallet.events`) |

### WebSocket protokolü

`ws://host/ws?userId=42&deviceId=phone` (kimlik demo amaçlı. Üretimde el sıkışmada JWT doğrulanır.)

| İstemci → sunucu | Sunucu → istemci |
|---|---|
| `SEND {clientMsgId, conversationId, body}` (sadece metin; para isteği HTTP ile ödeme servisine) | `SENT {clientMsgId, seq, serverTime, duplicate}` (✓) |
| `ACK {conversationId, seq}` | `MSG {message}` |
| `READ {conversationId, seq}` | `RECEIPT {conversationId, userId, seq, receipt: DELIVERED\|READ}` |
| `SYNC {cursors: {sohbet: seq}}` | `SYNC_RESULT {conversations: [...]}` |
| `HEADS` | `HEADS_RESULT {conversations: [{conversationId, lastSeq, readSeq}]}` |
| `TYPING {conversationId}` | `TYPING {conversationId, userId}` |
| `PRESENCE_QUERY {userIds}` | `PRESENCE {users}` |
| `PING` | `PONG`, `RECONNECT {afterMs}` (gateway kapanıyor), `RESYNC {afterMs}` (hızlı yol kesildi, senkronla), `ERROR {code, message, ref}` |

### Çalıştırma

```bash
docker compose up --build        # 2 PostgreSQL (chat, payreq) + Redis + Kafka + 2 gateway (8081, 8082) + worker + payment-request-service (8090)
```

Dockerfile'ların build context'i 007 klasörü (üst pom iki modülü birlikte bilir): `docker build -f payment-request-service/Dockerfile .`

Kubernetes: `kubectl apply -k k8s/` (gateway ×25 + HPA + PDB, worker ×3, payment-request-service ×2 + PDB, iki PostgreSQL, Redis, Kafka, Ingress, NetworkPolicy). Ingress `/v1/payment-requests`'i ödeme servisine, gerisini gateway'lere yollar. Ödeme veritabanına sadece ödeme servisi erişebilir.

### Testler

```bash
# 007 klasöründe (iki modül birlikte)
export IT_PG_URL=jdbc:postgresql://localhost:5432/chat IT_PG_USER=postgres IT_REDIS_HOST=localhost IT_REDIS_PORT=6379
mvn verify      # Docker varsa env gerekmez (Testcontainers); Kafka her zaman gömülü
                # ödeme servisi aynı sunucuda "payreq" veritabanını kullanır (yoksa testler oluşturur)
```

Sohbet entegrasyon testleri **iki ayrı Spring uygulaması** (gw-a ve gw-b) ayağa kaldırır. İkisi aynı PostgreSQL, Redis ve Kafka'ya bağlıdır, yani gerçek bir mini cluster. Drain testi üçüncü bir gateway açıp kapatır. Uçtan uca test gerçek `payment-request-service`'i de (kendi veritabanıyla, aynı Kafka'ya bağlı) aynı JVM'de başlatır.

| Test | Kanıtladığı |
|---|---|
| `CrossGatewayDeliveryIT` | Ali gw-a'da, Ayşe gw-b'de: mesaj Pub/Sub ile geçer. ✓, ✓✓, okundu geri döner. Relay'lerden tam olarak biri lider. Yabancı ve hatalı çerçeveler hata alır, bağlantı kopmaz |
| `IdempotencyAndOrderingIT` | Aynı mesaj iki gateway'den üç kez gönderilir: tek satır, hepsine aynı seq, sayaç boşa artmaz, Ayşe bir kez görür. İki kişi farklı gateway'lerden aynı anda 30'ar mesaj atar: seq 1–60 boşluksuz, iki farklı gateway'deki üyeler aynı sırayı görür |
| `OfflineAndMultiDeviceIT` | Çevrimdışıya push, dönüşte imleçten senkron (yeni cihaz, kayıtlı cihaz imleci, istemci imleci). Ölü gateway'deki bayat kayıt yok sayılır. Bir cihazda okunan mesaj diğerinde de okundu olur |
| `PaymentRequestServiceIT` (ödeme modülü) | Oluşturma idempotent, tek `CREATED` olayı tam anlık görüntüyle. İki cihazdan aynı anda "Öde": para bir kez, sürüm 1 → 3. Cüzdan cevabı kaybolunca reconciler tamamlar, para bir kez. Bakiye yetersiz → istek serbest kalır, tekrar ödenir. Ret ve süre dolumu kalıcı. Cüzdan olayıyla dışarıdan ödeme ve iade (olaylar iki kez gelse de bir kez işlenir, sürümler artan). Cüzdan olayı senkron cevaptan önce gelir: tek `PAID` olayı. Tutarı uyuşmayan/ilgisiz olaylar yok sayılır. Yetki ve doğrulama |
| `PaymentCardsIT` | Test ödeme servisinin rolünü oynar. Tekrar gelen olaylar tek kart + tek durum mesajı. **Sıra karışıklığı:** `PAID` (v3) `CREATED`'dan (v1) önce gelir, kart yine doğru ve tek. Üye olmayanın isteği, LINK kaynaklı ve bozuk referanslı olaylar sohbete düşmez. Soket artık para isteği kabul etmez |
| `PaymentEndToEndIT` | İki gerçek servis: HTTP ile iste → kart iki tarafa düşer. İki cihazdan aynı anda HTTP ile öde → para bir kez, sohbete tek "✅ ödendi". Cüzdan cevabı kaybolur → reconciler kapatır, sohbet olaydan öğrenir. Ret sohbette görünür |
| `BroadcastPresenceIT` | Kampanya kanalı iki gateway'deki abonelere ulaşır, push yok, abone yazamaz, geç gelen senkronla alır. "Yazıyor" iletilir ama saklanmaz. Presence: son görülme ve gizlilik |
| `RedisOutageIT` | **A.** Redis 4 sn donduruluyor, mesaj bu sırada gönderiliyor: beklemeli tekrar deneme sayesinde canlı yoldan teslim edilir (tekrar deneme kapatılınca bu test başarısız oluyor, denendi). **B.** Redis kapatılıyor ve **boş** açılıyor: gönderme çalışır, çevrimdışına push gider, gateway sondası toparlanır, rehberi yeniden doldurur, RESYNC gönderir, istemci HEADS + SYNC ile kaçırdığını alır, sonraki mesaj yine canlı gelir |
| `GatewayDrainIT` | Boşaltılan gateway 30 istemciye 0–2 sn'ye yayılmış RECONNECT gönderir, rehberden siler, yeni bağlantıya 503 döner |
| `AdmissionLimiterTest` | 5.000 eşzamanlı el sıkışmadan sadece kova kadarı kabul edilir, Retry-After rastgele |

**Sonuç** (yerel PostgreSQL 16 + Redis 7, gömülü Kafka): ödeme modülü 9 entegrasyon testi, sohbet 2 birim + 21 entegrasyon testi geçti (son tam koşu: `BUILD SUCCESS`). **Kırılganlık notu:** bu değişiklikten sonra sohbet testleri 13 kez çalıştırıldı; 9'u tamamen temiz. Kalan 4'ün hepsi zamanlamaya duyarlı, bu değişiklikte dokunulmayan testlerde: üçü `RedisOutageIT` / hemen ardından gelen test (Redis geri geldikten sonraki ilk mesaj 15–20 sn içinde canlı yoldan gelmedi), biri `AdmissionLimiterTest`. Önceki commit aynı makinede 7/7 temiz geçti; fark kesin olarak açıklanamadı. Olası sebep (doğrulanmadı): Redis yeniden bağlanırken fan-out'un yayını, gateway'in Pub/Sub aboneliği yenilenmeden yapılırsa mesaj canlı yoldan kaybolur ve sadece RESYNC → SYNC ile gelir; test istemcisi RESYNC'e cevap vermediği için bekler. `RedisOutageIT` Redis'i yerel süreç olarak yönettiği için sadece `IT_REDIS_HOST=localhost` ve `redis-server` PATH'teyken çalışır; Testcontainers ile atlanır.

**Testlerin yakaladığı gerçek hatalar:**
- Spring Kafka'nın varsayılan hata işleyicisi kısa bir Redis kesintisinde olayları beklemeden atlıyordu (§5.10).
- Spring'in Redis dinleyicisi her Pub/Sub mesajını ayrı bir thread'de işliyordu ve gw-b'deki üye grup mesajlarını farklı sırada gördü. Cihaz şeritleriyle düzeltildi (§5.1).

## 7. Dürüst notlar

- `docker-compose.yml` ve `k8s/` bu ortamda çalıştırılmadı (Docker ve Kubernetes yok). Sadece YAML sözdizimi kontrol edildi.
- Kimlik doğrulama demo amaçlı (sorgu parametresi ve `X-User-Id`). Üretimde JWT.
- Cüzdan (006) ve push (004) arayüz olarak tanımlı. Testlerde sahte uygulamaları kullanılıyor. `wallet.events` topic'inin sahibi cüzdan servisi: ödeme servisi onu oluşturmaz, sadece dinler (testte gömülü Kafka'da önceden açılıyor). `payment.events`'in sahibi ödeme servisi.
- `V3__payment_cards.sql` eski `payment_requests` tablosunu taşımadan siliyor (demo). Üretimde: veriyi yeni servise kopyala (backfill), bir süre iki taraf birlikte çalışsın, sonra eskiyi sil (expand → migrate → contract).
- Ödeme servisinin HTTP API'sinde kimlik demo amaçlı `X-User-Id`. Üretimde JWT; ayrıca istek oluşturma için hız limiti (spam para isteği).
- Tek PostgreSQL, tek Redis. Shard'lama, aylık partition ve Pub/Sub için ayrı Redis tasarımda anlatıldı (§5.3, §5.11), kodda yok.
- HPA'nın bağlantı sayısı metriği için Prometheus Adapter kurulumu gerekir.
