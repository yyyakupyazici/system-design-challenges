# Challenge Index

| # | Tarih | Konu | Kategori | Zorluk | Klasör |
|---|-------|------|----------|--------|--------|
| 001 | 2026-10-15 | Çok kiracılı dağıtık rate limiter (token bucket, Redis Lua, kural yayılımı) | Klasik | ⭐⭐⭐ Orta | [challenges/001-multi-tenant-rate-limiter](challenges/001-multi-tenant-rate-limiter/) |
| 004 | 2026-10-29 | Çok kanallı bildirim platformu (öncelikli topic'ler, router, öncelikli sağlayıcı kotası, idempotent teslimat) | Klasik | ⭐⭐⭐⭐ Orta-İleri | [challenges/004-multi-channel-notification-platform](challenges/004-multi-channel-notification-platform/) |
| 005 | 2026-11-05 | Kurumsal LLM Gateway (bütçe rezervasyonu, prompt caching, exact + semantic cache, PII maskeleme, gizli veri → kendi GPU) | AI | ⭐⭐⭐⭐ Orta-İleri | [challenges/005-llm-gateway](challenges/005-llm-gateway/) |
| 007 | 2026-11-07 | Gerçek zamanlı mesajlaşma ve presence (WebSocket gateway, Redis rehber + Pub/Sub, sohbet başına seq, idempotent gönderim, outbox + Kafka fan-out, senkron; para isteği ayrı servis + olaylardan kart) | Klasik | ⭐⭐⭐⭐⭐ İleri | [challenges/007-realtime-messaging](challenges/007-realtime-messaging/) |

## Lab'lar

Belirli bir teknolojiyi bütün kullanım senaryolarıyla, temelinden ele alan çalışmalar.

| Lab | Tarih | Konu | Klasör |
|---|---|---|---|
| Redis Architect Lab | 2026-11-06 | Redis'i mimar gözüyle: atomiklik (tek komut, Lua, MULTI, WATCH), 14 senaryo (cache, kilit + fencing, rate limit ×3, idempotency, flash sale stok, liderlik, gecikmeli kuyruk, Streams + DLQ, Pub/Sub, oturum, HLL/Bitmap/Geo, sıcak key, cluster), k8s'te 6 düğümlü Redis Cluster, gerçek failover ölçümü | [labs/redis-architect-lab](labs/redis-architect-lab/) |
