# #004 — Çok Kanallı Bildirim Platformu

**Kategori:** Klasik · **Zorluk:** ⭐⭐⭐⭐ Orta-İleri · **Tarih:** 2026-10-29

Öncelikli Kafka topic'leri, router, öncelikli sağlayıcı kotası (Redis + Lua) ve idempotent teslimat.

---

## 1. Senaryo

**Paramix**, 18 milyon kullanıcılı bir dijital banka ve cüzdan uygulaması. Şirketteki 40'tan fazla ekip bildirimlerini kendisi gönderiyordu ve bu bir kaosa dönmüştü:
- Aynı kampanya SMS'i kullanıcıya 3 kez gidiyordu.
- OTP kodu bazen 2 dakika gecikiyordu.
- Gece 03:00'te pazarlama push'u gidiyordu.

İstenen, bütün ekiplerin kullanacağı **merkezi bir bildirim platformu**.

- **Kanallar:** SMS (iki sağlayıcı), e-posta, push (FCM/APNs). Uygulama içi gelen kutusu bu projenin kapsamı dışında.
- **Öncelikler:**
  - **critical:** OTP, şüpheli işlem.
  - **transactional:** "Hesabına 500 TL geldi".
  - **marketing:** Kampanyalar.

## 2. Gereksinimler

| Metrik | Hedef |
|---|---|
| Normal / tepe trafik | 8.000 / 60.000 bildirim/sn |
| OTP gecikmesi (API → sağlayıcı) | p99 < 2 sn, **kampanya sırasında bile** |
| İşlemsel bildirim gecikmesi | p99 < 10 sn |
| Kampanya | 6 milyon bildirim ≤ 30 dk |
| Mükerrer gönderim | Pratikte sıfır (SMS'in tanesi para) |
| Geçmiş verisi | 13 ay |
| Erişilebilirlik | %99,95 (OTP için %99,99) |

**Bilerek konan zorluklar:**
1. Noisy neighbor (kampanya OTP'yi bekletmesin).
2. SMS sağlayıcısının limiti (2.000/sn) ve kesintisi.
3. Exactly-once yanılgısı.
4. Sıralama.
5. İzin kuyruktayken geri çekilirse.
6. Geçmiş verisinin hacmi (günde ~400 milyon olay).
7. Kampanyayı anında durdurma.

## 3. Yakup'un yorumları (tartışmanın özeti)

- **İlk fikir: "SMS, OTP, mail için ayrı Kafka topic'leri."** Doğru yön, çünkü izolasyon topic'te başlıyor. Ama SMS bir *kanal*, OTP bir *öncelik*, iki eksen karışmıştı. Kanala göre ayırırsak kampanya SMS'i ile OTP SMS'i aynı kuyrukta bekler.
- **"SMS ile OTP'yi ayırmak lazım, farklı topic."** ✅ Bunu genelleştirdik: girişte **önceliğe göre** 3 topic var. Teslimat tarafında **kanal × öncelik** (9 topic) var, böylece yavaş bir kanal diğerlerini bekletmiyor.
- **"429 almamak için rate limiti nasıl yönetiriz?"** Sağlayıcıya gitmeden önce `acquire` ile Redis'teki öncelikli kovalardan jeton alınıyor (001'deki token bucket'ın öncelik versiyonu). Bunun üzerine Redis'in single thread oluşunu, Lua'nın atomikliğini ve Redis Cluster'da hash tag kullanımını konuştuk.
- **"Uygulama silinip yüklenince token'ı event ile takip edip eskiyi yeniyle ezerim."** İyi fikir, üç düzeltmeyle uygulandı:
  1. Silinme anı yakalanamaz. Yakalanan an, uygulamanın yeni token'ı kaydettiği an.
  2. Token kullanıcı bazında değil, **kurulum** bazında tutulmalı (birden fazla cihaz olabilir).
  3. Cache'e yeni değer **yazılmaz**, cache **silinir** (event'ler sırasız gelebilir).
- E-posta ve push'ta da aynı kurgu geçerli. Değişen şey neyi koruduğumuz: SMS'te limit ve maliyet, e-postada gönderici itibarı, push'ta kullanıcının sabrı.

## 4. Önerilen tasarım

![mimari](design/architecture.mmd)

```
Ekipler ─► notify-api ──(aynı tx: notification + outbox)──► PostgreSQL
                                                              │
notify-scheduler (outbox relay) ◄─────────────────────────────┘
        │
        ▼
 notif.critical / notif.transactional / notif.marketing        ← GİRİŞ: önceliğe göre
        │
        ▼
 notify-router  (tercih, izin, sessiz saat, kanal seçimi, şablon; delivery satırı = PENDING)
        │
        ▼
 sms.* / email.* / push.*  (kanal × öncelik)                   ← TESLİMAT
        │
        ▼
 notify-worker-{sms,email,push} ─► claim ─► acquire (Redis kota) ─► sağlayıcı (client_ref)
```

**Tek imaj, dört rol** (`ROLE_API`, `ROLE_ROUTER`, `ROLE_WORKER`, `ROLE_SCHEDULER`). Kubernetes'te her rol ayrı bir Deployment olarak ölçekleniyor. Lokalde hepsi tek süreçte çalışıyor.

### 4.1 İzolasyon üç katmanda

| Katman | Nasıl |
|---|---|
| **Kuyruk** | Öncelik başına giriş topic'i, kanal × öncelik başına teslimat topic'i. |
| **İşlem gücü** | Öncelik başına ayrı consumer grubu ve thread havuzu. Kanal başına ayrı worker Deployment'ı. |
| **Dış kaynak** | Sağlayıcı önünde öncelikli kota: critical'ın garantisi var, marketing'in tavanı var. |

### 4.2 Router

- **Mükerrer koruma:** `delivery_id = notification_id + ":" + kanal` deterministik. Aynı mesaj iki kez işlense de satır bir kez oluşuyor.
- **Kullanıcı profili:** Redis'te **cache-aside** ile tutuluyor. Tercih ya da cihaz değişince key **siliniyor**, TTL de son emniyet ağı.
- **Kurallar** (`RoutingPolicy`, birim testli):
  - **critical:** Tercih ve sessiz saat dinlemiyor.
  - **transactional:** Kanal kapalıysa o kanaldan gitmiyor.
  - **marketing:** Pazarlama izni şart. Sessiz saatteyse `SCHEDULED` oluyor (kullanıcının saat dilimine göre).
  - **`fallback=true`:** İlk kanal hemen gidiyor, diğerleri `FALLBACK_WAIT` durumunda bekliyor. Örneğin push 10 sn içinde ack almazsa SMS gönderiliyor. **İçerik aynı:** OTP kodunu Auth servisi bir kez üretiyor, yedek kanal yeni kod üretmiyor.
- **Şablonlar:** Pod belleğinde tutuluyor. Eksik parametre hata kabul ediliyor, yarım mesaj gönderilmiyor.
- **Teslimat topic'lerinde key = `userId`:** Aynı kullanıcının bildirimleri aynı partition'da sırayla işleniyor (zorluk #4).

### 4.3 Öncelikli sağlayıcı kotası: `acquire`

![acquire](design/quota-acquire.mmd)

Kapasite iki parçaya bölünüyor:

```
smsA (limit 2.000 → güvenli 1.900/sn)
 ├─ critical garanti        400   ← sadece critical kullanır
 ├─ transactional garanti   500   ← sadece transactional kullanır
 ├─ marketing garanti         0
 └─ paylaşılan havuz      1.000   ← herkes TAVANINA kadar ödünç alır
    tavanlar: critical 1.400, transactional 1.500, marketing 1.000
```

- **Tek bir Lua çağrısı, atomik.** Redis komutları tek thread'de sırayla çalıştırıyor, script de tek bir komut sayılıyor.
- **Bütün key'ler `q:{smsA}:…` hash tag'li.** Redis Cluster'da hepsi aynı slot'ta ve aynı node'da, bu yüzden `CROSSSLOT` hatası çıkmıyor.
- **Zaman Redis'in `TIME` komutundan alınıyor.** Pod saatleri arasındaki farklar etkilemiyor.
- **Red cevabı `bekleme_ms` içeriyor.** Worker Redis'e sürekli sormak yerine o kadar uyuyor.
- **Config başlangıçta doğrulanıyor:** `Σ garanti ≤ global` ve `garanti ≤ tavan ≤ garanti + paylaşılan havuz`.

> ⚠️ **Tartışmadan farklı olan nokta (testle bulundu):** Chat'te ödünç almayı **global kovadan** yapıyorduk. Gerçek Redis'le test edince şunu gördük: Marketing havuzu bitirdikten sonra critical da ödünç aldı ve **transactional'ın 20'lik garantisinden geriye 1 jeton kaldı.** İki öncelik birlikte üçüncünün garantisini yiyebiliyordu. Düzeltme: ödünç alma sadece **paylaşılan havuzdan** (global − Σ garanti) yapılıyor. Artık garantiler gerçekten garanti.
>
> **Bedeli:** Boştaki garanti kapasitesi başkasına ödünç verilmiyor. Örneğin gece critical boşken onun 400'lük payı marketing'e geçmiyor. OTP'ye her zaman yer kalması için bilinçli seçilmiş bir trade-off. (`QuotaScriptIT` ikisini de doğruluyor.)

**Jeton alınamazsa ne oluyor?** (`DeliveryPolicy`)

| | Kota bekleme | Yedek sağlayıcı | UNKNOWN + sorgulanamıyor |
|---|---|---|---|
| critical | 300 ms | evet | tekrar gönder |
| transactional | 2 sn | evet | tekrar gönder |
| marketing | 30 sn (partition bekler = backpressure) | hayır | gönderme |

**Yine de 429 gelirse:** AIMD devreye giriyor. Efektif hız ×0,7 ile düşüyor, 30 sn 429 gelmezse +50/sn ile toparlanıyor. Hız Redis'te tutulduğu için bütün pod'lar birlikte yavaşlıyor. Bunun yanında sağlayıcı başına bir **circuit breaker** var (Resilience4j).

**Redis çökerse:**
- **critical ve transactional:** Pod içi yerel kovaya geçiyor (`garanti / pod sayısı`).
- **marketing:** Duruyor (fail-closed).

### 4.4 İdempotency: dört katman

```
① API      idempotency_key UNIQUE + transactional outbox      → aynı istek = aynı bildirim
② Router   delivery_id = notification_id:KANAL                 → aynı bildirim = aynı teslimat
③ Worker   koşullu UPDATE ile sahiplenme (PENDING → SENDING)
           + sağlayıcıya client_ref
           + Timeout = "gitti mi bilmiyorum" → UNKNOWN → sağlayıcıya sor (reconcile)
④ Webhook  provider_msg_id + durum sadece ileri gider
```

**Exactly-once mümkün değil.** Sağlayıcıya istek gidip cevap kaybolursa, mesajın gidip gitmediğini bilemezsin. Pratikte "bir kez" şöyle sağlanıyor:
- At-least-once teslimat.
- Her katmanda deterministik ID.
- Sağlayıcıya durum sorgusu.
- Kalan küçük risk için **önceliğe göre bilinçli karar:** İki kez gelen OTP zararsız (aynı kod), iki kez giden kampanya SMS'i zararlı.

Teslimatın geçtiği durumlar `design/delivery-state.mmd` içinde:

![durumlar](design/delivery-state.mmd)

Pod gönderirken ölürse **reaper** devreye giriyor:
- Sağlayıcı seçilmeden öldüyse mesaj kesin gitmemiştir: `PENDING`'e alınıp tekrar yayınlanıyor.
- Sağlayıcı seçildikten sonra öldüyse: `UNKNOWN` yapılıyor ve sağlayıcıya soruluyor.

Reaper'ın geçişleri "kilit gerçekten dolmuş mu" şartını kontrol ediyor. Böylece arada mesajı yeniden sahiplenmiş bir worker'ın işi bozulmuyor.

**Mesaj asla sessizce kaybolmaz** (`KafkaErrorHandlers`, `DeliveryRequeue`):
- **Geçici hata (DB, Kafka, Redis):** Router ve worker kaydı 0,5 sn'den 10 sn'ye artan aralıklarla **sınırsız** tekrar dener. Offset commit edilmez. Sadece bozuk mesajlar (JSON, geçersiz alan) atlanır.
- **"PENDING yap + teslimat topic'ine yaz" işlemi:** Zamanlayıcı, reconcile ve reaper'da iki ayrı adım olarak yapılsaydı, arada bir çökme satırı kuyruksuz bir `PENDING` olarak bırakırdı. Bu yüzden bu işlem **aynı transaction'da outbox'a** yazılıyor. Router ise yüksek hacimli olduğu için doğrudan Kafka'ya yazıyor. Yazma başarısız olursa offset commit edilmiyor ve mesaj tekrar işleniyor; deterministik `delivery_id` sayesinde bu zararsız.

### 4.5 Kampanyayı durdurma ve iznin geri çekilmesi (zorluk #5 ve #7)

- **Durdurma:** `POST /campaigns/{id}/stop` sadece DB'deki durumu değiştiriyor. Kuyruktaki milyonlarca mesaj silinmiyor. Router ve worker her mesajda kampanya durumuna bakıyor (2 sn'lik bellek cache'i) ve durmuş kampanyanın mesajlarını **atlıyor**.
- **İzin:** Router'da kaba bir eleme yapılıyor. Worker'da ise göndermeden hemen önce izin **cache'siz** olarak tekrar kontrol ediliyor. Mesaj kotayı beklerken izin geri çekildiyse gönderilmiyor.

### 4.6 E-posta ve push farkları

| | SMS | E-posta | Push |
|---|---|---|---|
| Asıl darboğaz | Sağlayıcı hız limiti | Alıcı tarafında itibar ve throttling | Bağlantı havuzu |
| Ek kota boyutu | – | Günlük kota + alıcı domain'i (gmail.com...) | Kullanıcı/cihaz başı limit |
| Özel konular | Maliyet, iki OTP | Ayrı IP/alt alan adı (OTP ↔ kampanya), bounce ve suppression list | Ölü token temizliği, `collapse_key`, high/normal öncelik |

Kodda üç kanal da aynı `acquire` mekanizmasını kullanıyor. Tablodaki ek boyutlar (domain kovası, cihaz başı limit) bir sonraki adım olarak bırakıldı (bkz. §7).

## 5. Veri modeli

`app/src/main/resources/db/migration/V1__notification_schema.sql`

| Tablo | Amaç |
|---|---|
| `notification` | `idempotency_key UNIQUE`, istek payload'u (JSONB) |
| `outbox` | Kafka'ya yazılacaklar. `notification` ile aynı transaction'da yazılıyor. |
| `delivery` | Bildirim × kanal. Durum makinesi, render edilmiş içerik, `provider` / `provider_msg_id`, `attempt`, `locked_until`, `not_before`. |
| `device_token` | **Kurulum** bazında push token'ları |
| `user_preference` | Pazarlama izni, kanal tercihleri, sessiz saat |
| `template` | Şablon × dil × kanal |
| `campaign` | `RUNNING` / `STOPPED` |

## 6. API

| Uç | Açıklama |
|---|---|
| `POST /notifications` | 202 döner, gönderim asenkron. Aynı `idempotencyKey` gelirse aynı `notificationId` döner, `duplicate: true` olur. |
| `GET /notifications/{id}` | Bildirim ve teslimat durumları (müşteri hizmetleri için) |
| `POST /webhooks/{provider}/dlr` | Sağlayıcı teslim raporu (idempotent) |
| `POST /deliveries/{id}/ack` | Uygulama push'u gösterdi. Bekleyen SMS yedeğini iptal eder. |
| `POST /campaigns`, `POST /campaigns/{id}/stop` | Kampanya oluşturma ve durdurma |
| `POST /devices` | Cihaz token'ı kaydı (cache invalidate) |
| `PUT /users/{id}/preferences` | Tercih ve izin güncelleme (cache invalidate) |

İstek örneği:

```json
{
  "idempotencyKey": "otp:login_7781",
  "type": "OTP_LOGIN",
  "priority": "CRITICAL",
  "userId": "u_1",
  "templateId": "otp_login",
  "params": { "code": "482913" },
  "channels": ["PUSH", "SMS"],
  "fallback": true
}
```

## 7. Trade-off'lar ve henüz kapsanmayanlar

**Bilinçli seçimler:**
- **Boştaki garanti ödünç verilmiyor.** Garantinin sağlamlığı için kapasiteden feragat ediliyor (§4.3).
- **Teslimat mesajı Kafka'ya iki kez yazılabilir.** Outbox relay ve router at-least-once çalışıyor. Worker'daki koşullu UPDATE bunu eliyor.
- **Marketing kotası beklerken partition'ı bekletiyor.** Bu bilinçli bir backpressure. Bunun için `lease` (60 sn), en uzun kota beklemesinden (30 sn) daha uzun tutuluyor.
- **Kampanya durumu 2 sn'lik cache'te.** "Durdur" en geç 2 sn içinde etkili oluyor. Daha hızlısı gerekirse Redis Pub/Sub kullanılabilir.

**Henüz kapsanmayanlar (devam edecek):**
- **Zorluk #6, geçmiş verisi:** Günde ~400 milyon olay 13 ay saklanacak. Aday çözümler: `delivery` tablosunu zamana göre partition'lamak, sıcak veriyi (son 30 gün) PostgreSQL'de, gerisini ClickHouse/S3'te (Parquet) tutmak.
- **Hot user / partition:** Tek bir kullanıcıya yoğun bildirim gelmesi durumu.
- **Gerçek sağlayıcı adaptörleri:** SMS operatörü, SES, FCM, APNs. `ChannelProvider` arayüzü hazır, şimdilik `FakeProvider` kullanılıyor.
- **E-posta için alıcı domain kovası, push için cihaz başı limit**, ölü token'ların (`UNREGISTERED`/`410`) otomatik silinmesi.
- **Kampanya genişletici:** Segmenti parçalara bölüp `notif.marketing`'e kontrollü hızla yazacak bileşen.
- **DLQ topic'leri:** Şu an kalıcı hatalar loglanıp atlanıyor. Webhook imza doğrulaması ve consumer lag'e göre ölçekleme (KEDA, `k8s/31-router.yaml`'da örnek var) de bu listede.

## 8. Kod

```
app/
  pom.xml, Dockerfile, docker-compose.yml
  src/main/java/io/paramix/notify/
    api/         NotificationController, NotificationService (idempotency + outbox), Admin, Webhook
    router/      RoutingService, RoutingPolicy (saf), TemplateRenderer, UserDirectory (cache-aside)
    worker/      DeliveryProcessor, DeliveryPolicy (saf), Reconciler, WorkerContainers
    quota/       QuotaService (Lua), AimdController, LocalTokenBucket
    provider/    ChannelProvider, SendResult, FakeProvider, ProviderRegistry (+ circuit breaker)
    scheduler/   OutboxRelay, DelayedDispatcher, StuckDeliveryReaper
    store/       repository'ler (JdbcClient)
  src/main/resources/
    scripts/priority_quota.lua
    db/migration/V1__notification_schema.sql, V2__seed_demo_data.sql
k8s/       namespace, config, postgres/redis/kafka (demo), api/router/worker×3/scheduler, ingress, networkpolicy
design/    architecture.mmd, delivery-state.mmd, quota-acquire.mmd
```

## 9. Nasıl çalıştırılır

### Testler

```bash
cd app
mvn verify
```

- **Birim testleri (21):** Router kuralları, sessiz saat, şablon, teslimat politikası, kota config doğrulaması, yerel kova. Hiçbir altyapı gerektirmiyor.
- **Entegrasyon testleri (19):**
  - `QuotaScriptIT`: gerçek Redis.
  - `NotificationFlowIT`: PostgreSQL + Redis + gömülü Kafka, uçtan uca.
  - Docker varsa Testcontainers kendiliğinden devreye giriyor. Docker yoksa hazır servisler verilebilir:
    ```bash
    IT_REDIS_HOST=localhost IT_REDIS_PORT=6379 \
    IT_PG_URL=jdbc:postgresql://localhost:5432/notify IT_PG_USER=notify IT_PG_PASSWORD=notify \
    mvn verify
    ```
  - İkisi de yoksa entegrasyon testleri atlanıyor.

`NotificationFlowIT` şu senaryoları doğruluyor:
- API retry'ında aynı bildirim döner, her kanal bir kez gider.
- Teslim raporu üç kez gelse de zararsızdır, durum geri gitmez.
- Sağlayıcı gönderdikten sonra timeout olursa sağlayıcıya sorulur, **ikinci SMS gitmez**.
- Sorgulanamayan UNKNOWN durumunda OTP tekrar gönderilir, kampanya bırakılır.
- Birincil SMS sağlayıcısı çökerse yedeğe geçilir.
- Push ack gelmezse aynı kodla SMS gider, ack gelirse SMS atlanır.
- İzni olmayana pazarlama gitmez.
- Sessiz saatte bekleyen mesajın izni geri çekilirse gönderilmez.
- Durdurulan kampanyanın mesajları düşürülür.
- Müşteri hizmetleri bildirimin durumunu sorgulayabilir.

### docker-compose

```bash
cd app && docker compose up --build

# OTP: önce push, 10 sn içinde ack gelmezse aynı kodla SMS
curl -s localhost:8080/notifications -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"otp:demo-1","type":"OTP_LOGIN","priority":"CRITICAL","userId":"u_1",
  "templateId":"otp_login","params":{"code":"482913"},"channels":["PUSH","SMS"],"fallback":true}'

# Aynı isteği tekrar gönder → aynı notificationId, "duplicate": true
# Durumu izle:
curl -s localhost:8080/notifications/<notificationId> | jq

# Kampanya: izni olmayan u_2'ye gitmez (SKIPPED no_marketing_consent)
curl -s localhost:8080/notifications -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"cmp:demo:u_2","type":"PROMO","priority":"MARKETING","userId":"u_2",
  "templateId":"promo_generic","params":{"headline":"Fırsat","url":"https://px.example"},
  "channels":["SMS"],"campaignId":"cmp_demo"}'

# Kota kovalarını canlı izle
docker compose exec redis redis-cli --scan --pattern 'q:*'
```

### kind (Kubernetes)

```bash
cd app && docker build -t paramix/notification-platform:0.1.0 .
kind create cluster --name notify
kind load docker-image paramix/notification-platform:0.1.0 --name notify
kubectl apply -k ../k8s
kubectl -n notify get pods -w
kubectl -n notify port-forward svc/notify-api 8080:80
```
