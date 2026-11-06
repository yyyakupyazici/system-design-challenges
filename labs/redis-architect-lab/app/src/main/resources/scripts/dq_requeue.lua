-- Reaper: görünürlük süresi dolan (worker'ı ölmüş) işleri tekrar "ready"ye al.
-- KEYS[1] ready   KEYS[2] processing   Dönüş: geri alınan iş sayısı
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[2], id)
  redis.call('ZADD', KEYS[1], now, id)
end
return #ids
