--[[
  Öncelikli sağlayıcı kotası (hierarchical token bucket) — tek çağrıda, atomik.

  Kapasite iki parçaya bölünür:
    • her önceliğin GARANTİ kovası (reserved_p)      → sadece o öncelik kullanır
    • PAYLAŞILAN havuz = global − Σ reserved          → herkes tavanına kadar ödünç alır
  Toplam en fazla Σ reserved + paylaşılan = global olur; sağlayıcı limiti aşılmaz.
  Garantiler kimse tarafından yenemez (iki öncelik birlikte ödünç alsa bile).
  Bedeli: boştaki garanti kapasitesi başkasına ödünç VERİLMEZ (bkz. README).

  KEYS (hepsi aynı hash tag, ör. q:{smsA}:... → Redis Cluster'da aynı slot/node):
    1  paylaşılan havuz    q:{smsA}:shared
    2  garanti kovası      q:{smsA}:CRITICAL:res
    3  tavan kovası        q:{smsA}:CRITICAL:ceil
    4  efektif hız (AIMD)  q:{smsA}:rate        429 gelince düşürülür; yoksa config değeri

  ARGV:
    1  config global hız (saniyede)
    2  tüm önceliklerin garanti toplamı (saniyede)
    3  bu önceliğin garanti hızı (saniyede, 0 olabilir)
    4  bu önceliğin tavan hızı (saniyede)
    5  istenen jeton (genelde 1)
    6  key TTL (ms)

  Dönüş: { izin (1/0), bekleme_ms, kaynak (1=garanti, 2=paylaşılan havuz, 0=red) }
]]

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)   -- ms, Redis saati (pod saatleri farklı olabilir)

local configRate = tonumber(ARGV[1])
local eff = tonumber(redis.call('GET', KEYS[4]) or configRate)
if eff > configRate then eff = configRate end
if eff < 1 then eff = 1 end
local f = eff / configRate                       -- AIMD hızı düşürdüyse tüm kovalar aynı oranda küçülür

local sRate = (configRate - tonumber(ARGV[2])) * f
local rRate = tonumber(ARGV[3]) * f
local cRate = tonumber(ARGV[4]) * f
local n = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

-- Kovayı son çağrıdan beri geçen süre kadar doldur. Kapasite = 1 saniyelik hız (burst).
local function load(key, rate)
  local b = redis.call('HMGET', key, 'tokens', 'ts')
  local tokens = tonumber(b[1])
  local ts = tonumber(b[2])
  if tokens == nil or ts == nil then
    return rate
  end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  local refilled = tokens + elapsed * rate / 1000
  if refilled > rate then refilled = rate end
  return refilled
end

local s = load(KEYS[1], sRate)
local r = load(KEYS[2], rRate)
local c = load(KEYS[3], cRate)

local allowed, source = 0, 0
if r >= n and c >= n then
  -- 1) kendi garantisinden
  r = r - n; c = c - n
  allowed, source = 1, 1
elseif s >= n and c >= n then
  -- 2) paylaşılan havuzdan ödünç (tavanı aşmadan)
  s = s - n; c = c - n
  allowed, source = 1, 2
end

local function save(key, tokens)
  redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
  redis.call('PEXPIRE', key, ttl)
end
save(KEYS[1], s)
save(KEYS[2], r)
save(KEYS[3], c)

if allowed == 1 then
  return { 1, 0, source }
end

-- 3) red: en erken hangi yoldan jeton açılır? (garanti yolu veya paylaşılan havuz yolu)
local function waitFor(tokens, rate)
  if tokens >= n then return 0 end
  if rate <= 0 then return 1000000 end
  return math.ceil((n - tokens) * 1000 / rate)
end
local viaReserved = math.max(waitFor(r, rRate), waitFor(c, cRate))
local viaShared = math.max(waitFor(s, sRate), waitFor(c, cRate))
local w = math.min(viaReserved, viaShared)
if w > 1000 then w = 1000 end
if w < 1 then w = 1 end
return { 0, w, 0 }
