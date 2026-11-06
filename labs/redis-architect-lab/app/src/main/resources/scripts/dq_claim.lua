-- Zamanı gelen işleri al (claim). "Oku sonra sil" iki çağrı olsaydı iki worker aynı işi alabilirdi.
-- Alınan iş silinmez, "processing" ZSET'ine görünürlük süresiyle taşınır: worker ölürse iş kaybolmaz.
-- KEYS[1] ready   KEYS[2] processing   KEYS[3] jobs   ARGV[1] en fazla iş   ARGV[2] görünürlük süresi (ms)
-- Dönüş: {id1, payload1, id2, payload2, ...}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), id)
  table.insert(out, id)
  table.insert(out, redis.call('HGET', KEYS[3], id) or '')
end
return out
