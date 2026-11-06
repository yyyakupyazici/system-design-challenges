-- Rezervasyonu iptal et (ödeme olmadı / süre doldu): adet stoğa geri döner. İdempotent.
-- KEYS: stock_reserve.lua ile aynı   ARGV[1] sipariş id   Dönüş: iade edilen adet | 0
local qty = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
if qty == 0 then
  return 0
end
redis.call('INCRBY', KEYS[1], qty)
redis.call('HDEL', KEYS[2], ARGV[1])
return qty
