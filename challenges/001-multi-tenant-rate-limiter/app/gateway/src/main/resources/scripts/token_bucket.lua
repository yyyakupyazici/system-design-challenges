--[[
  Çok kovalı (multi-bucket) token bucket + günlük kota. Hepsi ya da hiçbiri (all-or-nothing).

  KEYS[1..n]   : kova key'leri (ör. rl:{tenant_42}:*  ve  rl:{tenant_42}:ep:POST /payments)
  KEYS[n+1]    : (opsiyonel) günlük kota key'i (ör. quota:{tenant_42}:2026-10-15)
                 Redis Cluster için tüm key'ler aynı hash tag'i ({tenant}) taşımalı → aynı slot.

  ARGV[1]              : n (kova sayısı)
  ARGV[2i], ARGV[2i+1] : i. kovanın rate (jeton/sn) ve burst (kapasite) değerleri
  ARGV[2n+2]           : günlük kota limiti (-1 = kota yok)
  ARGV[2n+3]           : kota key'i TTL (sn)

  Dönüş: { allowed, reason, remaining, limit, retryAfterMs, resetMs, quotaRemaining }
    reason: 0 = OK, 1 = RATE (kova boş), 2 = QUOTA (günlük kota doldu)
    quotaRemaining: -1 = kota tanımlı değil

  Notlar:
  - Zaman pod'lardan değil Redis'in kendi saatinden (TIME) alınır → pod'lar arası clock skew etkisizdir.
  - Kural değerleri (rate/burst) Redis'te saklanmaz; her çağrıda parametre gelir.
    Limit değişince math.min(burst, ...) kovayı kendiliğinden yeni kapasiteye uydurur.
  - Reddedilen istekte hiçbir kovaya yazılmaz; dolum ts üzerinden tembel (lazy) hesaplandığı
    için durum tutarlı kalır ve "yarım harcama" olmaz.
]]

local n = tonumber(ARGV[1])
local quotaLimit = tonumber(ARGV[2 * n + 2])
local quotaTtl = tonumber(ARGV[2 * n + 3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local tokens = {}
local rates = {}
local bursts = {}

local rateDenied = false
local retryAfterMs = 0
local bottleneck = 1

for i = 1, n do
  local rate = tonumber(ARGV[2 * i])
  local burst = tonumber(ARGV[2 * i + 1])
  rates[i] = rate
  bursts[i] = burst

  local state = redis.call('HMGET', KEYS[i], 'tokens', 'ts')
  local tk = tonumber(state[1])
  local ts = tonumber(state[2])
  if tk == nil or ts == nil then
    tk = burst            -- ilk istek: kova dolu kabul edilir
    ts = now
  end

  local elapsed = math.max(0, now - ts)
  tk = math.min(burst, tk + (elapsed * rate / 1000))
  tokens[i] = tk

  if tk < 1 then
    rateDenied = true
    local wait = math.ceil((1 - tk) * 1000 / rate)
    if wait > retryAfterMs then
      retryAfterMs = wait
      bottleneck = i
    end
  elseif not rateDenied and tk < tokens[bottleneck] then
    bottleneck = i
  end
end

-- Günlük kota (fixed window, takvim günü). Sadece limiter'dan geçen istekler sayılır.
local quotaRemaining = -1
local quotaDenied = false
if quotaLimit >= 0 then
  local used = tonumber(redis.call('GET', KEYS[n + 1])) or 0
  if used >= quotaLimit then
    quotaDenied = true
    quotaRemaining = 0
  else
    quotaRemaining = quotaLimit - used
  end
end

if rateDenied or quotaDenied then
  local reason = 1
  if quotaDenied then reason = 2 end
  local remaining = math.floor(math.max(0, tokens[bottleneck]))
  if rateDenied then remaining = 0 end
  local resetMs = math.ceil((bursts[bottleneck] - tokens[bottleneck]) * 1000 / rates[bottleneck])
  return { 0, reason, remaining, rates[bottleneck], retryAfterMs, resetMs, quotaRemaining }
end

-- Hepsinde jeton var → hepsinden 1 düş.
for i = 1, n do
  local left = tokens[i] - 1
  redis.call('HSET', KEYS[i], 'tokens', tostring(left), 'ts', tostring(now))
  -- Kova tamamen dolmuşsa key'e gerek yok; dolma süresinin 2 katı + 1 sn sonra silinsin.
  redis.call('PEXPIRE', KEYS[i], math.ceil(bursts[i] * 1000 / rates[i]) * 2 + 1000)
  tokens[i] = left
end

if quotaLimit >= 0 then
  local used = redis.call('INCR', KEYS[n + 1])
  if used == 1 then
    redis.call('EXPIRE', KEYS[n + 1], quotaTtl)
  end
  quotaRemaining = quotaLimit - used
end

local remaining = math.floor(tokens[bottleneck])
local resetMs = math.ceil((bursts[bottleneck] - tokens[bottleneck]) * 1000 / rates[bottleneck])
return { 1, 0, remaining, rates[bottleneck], 0, resetMs, quotaRemaining }
