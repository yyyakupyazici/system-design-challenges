# #005 — Kurumsal LLM Gateway

**Kategori:** AI · **Zorluk:** ⭐⭐⭐⭐ Orta-İleri · **Tarih:** 2026-11-05

Bütün ekiplerin kullandığı tek bir OpenAI-uyumlu uç. Arkasında birden fazla sağlayıcı, kendi GPU kümemiz, bütçe rezervasyonu, exact ve semantic cache, PII maskeleme ve kullanım defteri var.

---

## 1. Senaryo

Paramix'te 25 ekip LLM kullanıyor: müşteri destek asistanı, dolandırıcılık analizi, kod asistanı, kampanya metni, doküman özetleme. Her ekip sağlayıcıyı kendi anahtarıyla doğrudan çağırıyordu. Bunun sonuçları:

- **Fatura:** Ayda $340 bin. Hatalı bir döngü bir gecede $28 bin harcadı.
- **Kesinti:** Birincil sağlayıcı 40 dakika 529 döndü ve destek asistanı tamamen çöktü.
- **Güvenlik:** Prompt'lara TC kimlik ve kart numarası girdi.
- **Görünürlük:** Kimin neyi ne kadara kullandığını bilen yoktu.

## 2. Gereksinimler

| Metrik | Hedef |
|---|---|
| Tepe trafik | 300 istek/sn (ilk metindeki 3.000 maliyetle tutarsızdı, düzeltildi) |
| Ortalama istek | 1.500 girdi + 400 çıktı token |
| Gateway'in eklediği gecikme | p99 < 30 ms |
| İlk token süresi (destek) | p95 < 1,2 sn |
| Erişilebilirlik | %99,95, sağlayıcı kesintisinde bile |
| Maliyet | Faturayı %40 düşür |
| Semantic cache yanlış isabet | < %0,5 |
| Kendi GPU kümemiz | 8 GPU, ~6.000 çıktı token/sn |

**Zorluklar:**
1. Streaming'de maliyet ancak sonda belli oluyor.
2. Fallback tuzağı: yedek sağlayıcının kapasitesi ¼, ve stream ortasında kopma riski var.
3. Semantic cache tehlikesi: kişiye özel sorular ve sayıları farklı sorular.
4. PII maskeleme ve cevapta geri açma.
5. "Gizli" verinin sadece kendi GPU'muza gitmesi.
6. Noisy neighbor.
7. Model sürümü değişince cache'teki eski cevaplar.

## 3. Kapasite ve tasarruf (özet)

Ortalama ~15 istek/sn ile günde ~1,3 milyon istek, ~1,95 milyar girdi ve ~0,52 milyar çıktı token'ı işleniyor. Hepsi büyük modele giderse fatura **≈ $410 bin/ay** olur.

| Kaldıraç | Etki (varsayım) |
|---|---|
| Yanıt cache'i (exact + semantic) | İsteklerin ~%15'i sağlayıcıya hiç gitmez → $410 bin → $350 bin |
| `auto` yönlendirme | Kalanın yarısı ~12 kat ucuz küçük modele gider → ~$190 bin |
| Sağlayıcı tarafı prompt caching | Sohbetlerde tekrar eden önek ~%95 ucuza okunur |
| Kendi GPU kümemiz | Sabit maliyet. Gizli veri ve gece trafiği buraya gider |

Sadece ilk iki kaldıraç ~%54 tasarruf sağlıyor. Bu yüzden mimari **cache ve yönlendirme** etrafına kuruldu.

## 4. Mimari

Diyagramlar: [mimari](design/architecture.mmd) · [bütçe rezervasyon/mutabakat](design/budget-reserve-settle.mmd) · [stream fallback](design/stream-fallback.mmd)

Bir isteğin yolu (`core/GatewayService`):

```
① auth + girdi tahmini     ② PII maskele        ③ yönlendir (auto, gizli → internal, fallback zinciri)
④ cache (exact → semantic) → isabette ⑤–⑦ atlanır, maliyet 0
⑤ ekip token/dk limiti     ⑥ bütçe rezervasyonu (en fazla tutar)
⑦ sağlayıcı (circuit breaker + sağlayıcı kapasitesi; stream'de fallback sadece ilk token'dan önce)
⑧ PII geri aç              ⑨ mutabakat (gerçek tutar, iade) + cache yaz + kullanım defteri
```

Sıralamanın iki kuralı var:
- Ucuz ve reddedebilen adımlar önde.
- Veriyi değiştiren adım (maskeleme), veriyi dışarı çıkaran her adımdan önce: cache, sağlayıcı ve log maskeli metni görür.

## 5. Kararlar

### 5.1 Bütçe: rezervasyon + mutabakat (zorluk #1)

Otel mantığı: girişte kartından **en fazla tutar** bloke edilir, çıkışta gerçek tutar çekilir ve farkı iade edilir.

```
en fazla tutar = girdi × max(input fiyatı, cache-write fiyatı) + max_tokens × output fiyatı
```

- Cache indirimi varsayılmaz, çünkü cache'in süresi dolmuş olabilir. Çıktı `max_tokens` kadar sayılır. Bu yüzden stream ortasında bütçe **bitemez**: gereken para baştan ayrılmıştır.
- Fallback zincirinde **en pahalı aday** için rezerve edilir. Böylece yedeğe geçince bütçe aşılmaz.
- Para birimi **mikro-kredi**. Fiyatlar "1M token başına kredi" olduğu için `token × fiyat` doğrudan mikro-krediye eşit olur. Float yok.

**Paylaşılan bütçe (004'teki öncelikli kotanın aynısı):** Örnek ekip 10 kişi ve günlük 1.200 kredi.

| | Değer |
|---|---|
| Kişi başı garanti | 60 kredi (toplam 600) |
| Paylaşılan havuz | 1.200 − 600 = 600 kredi |
| Kişi başı tavan | 300 kredi (kural: tavan ≤ garanti + havuz, başlangıçta doğrulanır) |

Bir kişi önce kendi garantisini, sonra havuzu kullanır. Diğerleri havuzu bitirse bile kimsenin garantisine dokunulamaz. İade önce havuza yapılır, böylece başkalarının işine yarar.

Hepsi **tek bir Lua script'i** (`budget_reserve.lua`) ile atomik çalışır. Key'ler `b:{ekip:gün}...` biçiminde hash tag'li olduğu için Redis Cluster'da bir ekibin bütün key'leri aynı slotta durur.

**Kenar durumlar:**
- Aynı istek tekrar gelirse ikinci kez bloke edilmez. Settle idempotent'tir.
- Pod stream ortasında ölürse settle hiç gelmez. **Reaper**, süresi geçen rezervasyonu tam tutarla kapatır (temkinli taraf).
- %80 ve %100 eşiklerinde günde bir kez uyarı verilir.
- Bütçe dolunca ekibin seçimine göre `402 budget_exceeded` (REJECT) döner ya da istek **en ucuz modele düşürülür** (DOWNGRADE).
- Bütçe Redis'i çökerse toplu işler durur (503, fail-closed). Sohbetler devam eder (fail-open) ve metrikle izlenir.

### 5.2 Session ve prompt caching: maliyet nasıl hesaplanır?

Sağlayıcılar **durumsuz**. Her turda bütün geçmiş yeniden gönderilir, yani "session'ın toplamı" diye tek bir istek yoktur. Her tur ayrı bir istektir ve faturası şöyle hesaplanır:

```
maliyet = yeni girdi × input + cache'ten okunan × cacheRead + cache'e yazılan × cacheWrite + çıktı × output
```

IDE'deki fiyatlarla (200 / 1000 / 10 / 250) 3 turluk bir sohbet (`CostCalculatorTest`):

| | Cache'siz | Prompt caching'li |
|---|---|---|
| Toplam | 5,70 kredi | 4,29 kredi |

**Cache yazdırmak ne zaman açılır?** (`PromptCachePolicy`) Yazma normal girdiden pahalıdır (250 > 200). Kâr ancak aynı önek bir daha okunursa gelir:
- **BATCH** (her doküman bir kez işlenir) → hayır. Yazma primi boşa gider.
- **CHAT** + `X-Session-Id` ya da önceki asistan turları + önek ≥ 1.024 token → evet.

Anthropic'te `AnthropicCacheOptions` (CONVERSATION_HISTORY) ile açılır. Usage'daki `cache_creation` ve `cache_read` alanları ayrı ayrı faturalanır.

### 5.3 Yönlendirme ve fallback (zorluk #2)

- **`auto`:** Girdi ≥ 3.000 token ya da "analiz, kod, karşılaştır, neden..." geçiyorsa büyük model, gerisi küçük model.
- **Fallback zinciri** config'ten gelir (`small → small-backup`). Bir aday iki durumda atlanır:
  - Circuit breaker açıksa (20 çağrılık pencere, %50 hata ya da %80 yavaş çağrı).
  - Sağlayıcının **token/dk kapasitesi** doluysa. Yedeğin kapasitesi ¼ olduğu için bütün trafiğin ona akıp onu da çökertmesi bu kovayla engellenir. Kapasite yetmeyince istek kuyruğa değil, sıradaki adaya ya da 503'e gider.
- **Stream'de fallback sadece ilk token'dan önce yapılır.** Kullanıcıya bir şey gittiyse yarım cevabın üstüne başka modelin cevabı eklenemez. Kopmada:
  - Üretilen kadar ödenir.
  - `stream_interrupted` hata parçası ve `[DONE]` gönderilir.
  - Cevap cache'e yazılmaz.
- İlk token `firstTokenTimeout` (10 sn) içinde gelmezse de yedeğe geçilir.

### 5.4 Cache (zorluk #3 ve #7)

**Kural:** cevap SADECE soruya bağlıysa cache'lenir. Kişiye, kişinin verisine ya da bir eyleme bağlıysa asla (`CachePolicy`):

| Soru | Karar |
|---|---|
| "Kartımı nasıl iptal ederim?" | ✓ genel prosedür |
| "Hesabımda ne kadar para var?" | ✗ kişiye özel veri |
| "TR12… IBAN'a 500 TL gönder" | ✗ PII + eylem |
| "Bu ayki harcamalarımı özetle" | ✗ kişiye özel veri |
| `X-Data-Class: gizli`, uygulama opt-out, temperature > 0,3 | ✗ |

- **Exact:** Redis, 24 saat. Anahtar = ekip + uygulama + model + **sürüm** + parametreler + maskeli mesajlar.
- **Semantic:** Spring AI `VectorStore` (prod'da pgvector). Sadece tek soruluk isteklerde kullanılır. Yanlış isabete karşı üç savunma var:
  1. Benzerlik eşiği 0,92.
  2. Kapsam: ekip + uygulama + model + **model sürümü**. Sürüm değişince eski cevaplar hiç bulunmaz (zorluk #7). Yeni sürümün kalitesi önce gölge trafikle karşılaştırılır.
  3. **Sayı koruması:** "500 TL" ile "600 TL" vektör olarak neredeyse aynıdır. Sayılar birebir aynı değilse isabet sayılmaz.
- `finish_reason = length` olan (yarım kalmış) cevaplar cache'e yazılmaz.

### 5.5 PII ve veri sınıfı (zorluk #4 ve #5)

- **Maskeleme** regex + checksum ile yapılır (`PiiMasker`): TCKN algoritması, IBAN mod-97, kart Luhn, TR telefon, e-posta. Checksum sayesinde sipariş numarası gibi rastgele 11 haneli sayılar maskelenmez. Bu iş mikrosaniyeler sürer, 30 ms bütçesini yemez.
- Aynı değer aynı istekte hep aynı yer tutucuyu alır (`<TCKN_1>`). Eşleme sadece istek süresince bellekte durur.
- **Stream'de geri açma:** `<EMAIL_1>` iki parçaya bölünüp `<EM` + `AIL_1>` diye gelebilir. `StreamingUnmasker`, yer tutucu olabilecek kuyruğu tamamlanana kadar tutar.
- **Gizli veri:**
  - `auto` → internal (vLLM). Açıkça dış model istenirse `403 data_class_violation` döner.
  - Fallback zinciri de süzülür: gizli veri hiçbir koşulda dışarı çıkmaz.
  - Ağ seviyesinde ikinci savunma: vLLM'in hiç dış çıkışı yok (NetworkPolicy).
- **GPU kümesi dolarsa:** Gizli istekler için başka yer yok. Bu yüzden önceliği gizli verinin: destek trafiği GPU'ya sadece kapasite boşken gider, gizli istek beklerse 429 + `Retry-After` döner. Kodda şimdilik GPU için tek bir kapasite kovası var (`GPU_TPM`). Gizli trafiğe ayrılmış garanti payı (004'teki öncelikli kota gibi) bir sonraki adım.

### 5.6 Noisy neighbor (zorluk #6)

Üç katman var:
- **Ekip token/dk kovası:** Rezervasyon `girdi + max_tokens` kadar yapılır, sonra gerçek kullanıma göre iade edilir. Aşılırsa `429 + Retry-After`.
- **Ekip günlük bütçesi.**
- **Sağlayıcı kapasite kovası.**

Toplu özetleme job'u kendi kovasını doldurur ve durur, destek asistanı etkilenmez.

### 5.7 Kullanım defteri ve gözlemlenebilirlik

- `usage_event` tablosu: ekip, uygulama, kullanıcı, istenen/yönlendirilen model, sağlayıcı, fallback, cache durumu, prompt cache, dört token kalemi, maliyet, gecikme, TTFT, durum, maskelenen PII sayısı.
- **Asenkron** yazılır, istek beklemez. Kuyruk dolarsa yazan thread kendisi yazar: kayıt düşmez.
- Maskeli prompt ve cevap **30 gün** sonra silinir (gece job'u). Sayısal satırlar kalır.
- Ölçek büyüyünce olaylar Kafka → ClickHouse'a akar. Bütçenin gerçek kaynağı defter, Redis ise hızlı kopya.
- Metrikler (Prometheus): `llmgw.cache`, `llmgw.provider.error/skipped`, `llmgw.budget.denied/downgraded/reaped/alert`, `llmgw.cost.micro`.

### 5.8 Korkutan noktalar ve trade-off'lar

- **Token tahmini:** Girdi `karakter/4` ile tahmin ediliyor. Gerçek tokenizer'dan sapabilir. Rezervasyon bunu iade ile düzeltir, ama 4.000 token sınırındaki bir istek yanlış tarafa düşebilir.
- **Redis = para:** AOF açık. Yine de son 1 saniyelik harcama kaybolabilir. Gece mutabakatı defterden yapılır.
- **Semantic cache:** Eşik ve sayı koruması yanlış isabeti azaltır ama sıfırlamaz. İlk aylarda örneklenen isabetler insanlar tarafından etiketlenmeli ve eşik buna göre ayarlanmalı.

## 6. Kod

`app/` (Spring Boot 3.5, Java 21, Spring AI 1.1):

| Paket | Ne var |
|---|---|
| `api` | `/v1/chat/completions` (JSON + SSE), `/v1/usage`, OpenAI tarzı hata gövdesi |
| `auth` | Gateway anahtarı (SHA-256 özeti) → ekip/uygulama, `X-Data-Class`, `X-Session-Id` |
| `core/GatewayService` | Yukarıdaki ①–⑨ akışı; stream, fallback, mutabakat |
| `budget` + `scripts/*.lua` | Rezervasyon, mutabakat, reaper, uyarılar |
| `limit` | Token/dk kovası (ekip ve sağlayıcı) |
| `cache` | `CachePolicy`, exact (Redis), semantic (VectorStore), `PromptCachePolicy` |
| `pii` | Maskeleme, vault, stream geri açma |
| `routing` | `auto`, veri sınıfı, fallback zinciri, en ucuz model |
| `provider` | Spring AI `ChatModel` → Anthropic / OpenAI (vLLM dahil) / Ollama / FAKE |
| `usage` | Kullanım defteri (PostgreSQL), 30 gün içerik saklama |

Sağlayıcılar config'ten kurulur, tipi env ile değişir. API anahtarları sadece env/Secret'tan gelir.

### Çalıştırma

```bash
docker compose up --build        # sağlayıcılar FAKE, anahtar gerekmez
curl -s localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-support-demo' -H 'X-User-Id: u1' -H 'Content-Type: application/json' \
  -d '{"model":"auto","messages":[{"role":"user","content":"EFT ile havale farkı nedir?"}]}'
curl -s localhost:8080/v1/usage -H 'Authorization: Bearer sk-support-demo' -H 'X-User-Id: u1'
```

Gerçek sağlayıcı için `PRIMARY_PROVIDER_TYPE=ANTHROPIC`, `PRIMARY_API_KEY=...`, `LARGE_MODEL_ID=...` gibi değerleri verin. Kubernetes için `kubectl apply -k k8s/` (gateway, vLLM GPU örneği, NetworkPolicy, iç Ingress).

### Testler

```bash
cd app && mvn verify     # Docker varsa Testcontainers; yoksa IT_REDIS_HOST / IT_PG_URL ile hazır servisler
```

- **24 birim testi:**
  - PII (checksum, aynı değer = aynı yer tutucu, bölünmüş yer tutucu)
  - Maliyet (3 turluk prompt caching örneği)
  - Router
  - CachePolicy (a–f)
  - PromptCachePolicy
- **5 bütçe IT'si** (gerçek Redis):
  - Garanti → havuz → tavan
  - İadenin havuza gitmesi ve idempotent settle
  - Retry'da çift bloke olmaması
  - **50 eşzamanlı rezervasyonun bütçeyi aşamaması**
  - Reaper
- **15 uçtan uca IT** (Redis + PostgreSQL, FAKE sağlayıcı):
  - Exact ve semantic isabet, sayı koruması
  - PII'nin sağlayıcıya maskeli gidip cevapta açılması
  - SSE'de anlık geri açma
  - İlk token'dan önce hata ve yavaş ilk token'da fallback
  - Stream ortası kopmada yedeğe geçilmemesi
  - Gizli veri → GPU, dış model → 403
  - 402 ve DOWNGRADE
  - 429 + Retry-After
  - BATCH'te prompt cache kapalı, CHAT'te açık
