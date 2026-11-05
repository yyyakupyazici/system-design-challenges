--[[
  Bütçe rezervasyonu (otel mantığı: girişte "en fazla" tutar bloke edilir).
  Ekibin günlük bütçesi = üyelerin garantisi + paylaşılan havuz; kişi başı tavan var (004'teki kotanın aynısı).

  KEYS (hepsi aynı hash tag: b:{ekip:gün}... → Redis Cluster'da aynı slot):
    1 ekip hash        b:{team:day}            pool (havuzdan kullanılan), reserved, spent
    2 kullanıcı hash   b:{team:day}:u:<user>   own (garantiden kullanılan), pool (havuzdan kullandığı), spent
    3 rezervasyon      b:{team:day}:r:<id>     user, own, pool
    4 süre indeksi     b:{team:day}:rz         ZSET (skor = rezervasyonun son geçerlilik anı, ms)

  ARGV: 1 tutar (mikro-kredi), 2 kullanıcının garantisi (üye değilse 0), 3 havuz limiti, 4 kişi başı tavan,
        5 son geçerlilik (ms), 6 key TTL (sn), 7 kullanıcı, 8 rezervasyon id

  Dönüş: {1, garantiden, havuzdan} | {0, sebep}  (sebep 1 = kişi tavanı, 2 = ekip havuzu bitti)
  "Kullanılan" rezervasyonları da içerir: aynı anda açılan 50 stream birlikte bütçeyi aşamaz.
]]
if redis.call('EXISTS', KEYS[3]) == 1 then
  -- aynı istek tekrar geldi (retry): ikinci kez bloke etme
  return { 1, tonumber(redis.call('HGET', KEYS[3], 'own')), tonumber(redis.call('HGET', KEYS[3], 'pool')) }
end

local amount = tonumber(ARGV[1])
local guarantee = tonumber(ARGV[2])
local poolLimit = tonumber(ARGV[3])
local ceiling = tonumber(ARGV[4])

local ownUsed = tonumber(redis.call('HGET', KEYS[2], 'own') or '0')
local userPool = tonumber(redis.call('HGET', KEYS[2], 'pool') or '0')
local poolUsed = tonumber(redis.call('HGET', KEYS[1], 'pool') or '0')

if ownUsed + userPool + amount > ceiling then
  return { 0, 1 }
end

local ownFree = guarantee - ownUsed
if ownFree < 0 then ownFree = 0 end
local own = math.min(ownFree, amount)
local rest = amount - own
if rest > poolLimit - poolUsed then
  return { 0, 2 }
end

redis.call('HINCRBY', KEYS[2], 'own', own)
redis.call('HINCRBY', KEYS[2], 'pool', rest)
redis.call('HINCRBY', KEYS[1], 'pool', rest)
redis.call('HINCRBY', KEYS[1], 'reserved', amount)
redis.call('HSET', KEYS[3], 'user', ARGV[7], 'own', own, 'pool', rest)
redis.call('ZADD', KEYS[4], ARGV[5], ARGV[8])

local ttl = tonumber(ARGV[6])
for i = 1, 4 do redis.call('EXPIRE', KEYS[i], ttl) end
return { 1, own, rest }
