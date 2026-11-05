--[[
  Mutabakat (otel çıkışı: gerçek tutar çekilir, fark iade edilir). Idempotent: rezervasyon yoksa hiçbir şey yapmaz.

  KEYS: budget_reserve.lua ile aynı (2 numaralı key, rezervasyondaki kullanıcıya ait olmalı)
  ARGV: 1 gerçek tutar (mikro-kredi; -1 = bilinmiyor, rezerve tutarın tamamını harcanmış say), 2 rezervasyon id

  İade önce havuza yapılır (diğer üyelerin işine yarasın), sonra kişinin garantisine.
  Gerçek tutar rezervasyonu aştıysa (tahmin tutmadı) fark havuzdan düşülür ve bir defaya mahsus aşıma izin verilir.

  Dönüş: {1, ekibin toplam harcaması} | {0}
]]
-- Lua'da -0 "-0" diye yazılır ve HINCRBY onu sayı kabul etmez: sıfır artışları hiç gönderme.
local function incr(key, field, delta)
  if delta ~= 0 then redis.call('HINCRBY', key, field, delta) end
end

if redis.call('EXISTS', KEYS[3]) == 0 then
  return { 0 }
end
local own = tonumber(redis.call('HGET', KEYS[3], 'own'))
local pool = tonumber(redis.call('HGET', KEYS[3], 'pool'))
local reserved = own + pool
local actual = tonumber(ARGV[1])
if actual < 0 then actual = reserved end

local diff = reserved - actual
if diff >= 0 then
  local refundPool = math.min(diff, pool)
  local refundOwn = diff - refundPool
  incr(KEYS[2], 'pool', -refundPool)
  incr(KEYS[1], 'pool', -refundPool)
  incr(KEYS[2], 'own', -refundOwn)
else
  incr(KEYS[2], 'pool', -diff)
  incr(KEYS[1], 'pool', -diff)
end
incr(KEYS[1], 'reserved', -reserved)
incr(KEYS[1], 'spent', actual)
incr(KEYS[2], 'spent', actual)
redis.call('DEL', KEYS[3])
redis.call('ZREM', KEYS[4], ARGV[2])
return { 1, tonumber(redis.call('HGET', KEYS[1], 'spent')) }
