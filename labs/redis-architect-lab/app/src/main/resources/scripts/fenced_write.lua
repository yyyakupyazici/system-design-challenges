-- Fencing: depolama, gördüğü en büyük token'dan KÜÇÜK token'la gelen yazmayı reddeder.
-- Kilidin süresi dolmuş "zombi" sahip (GC duraklaması, ağ kopması) geç kalmış yazmasını yapamaz.
-- KEYS[1] veri hash'i (data:{kaynak})   ARGV[1] token   ARGV[2] değer
-- Dönüş: 1 yazıldı | 0 eski token, reddedildi
local last = tonumber(redis.call('HGET', KEYS[1], 'fence') or '0')
if tonumber(ARGV[1]) < last then
  return 0
end
redis.call('HSET', KEYS[1], 'fence', ARGV[1], 'value', ARGV[2])
return 1
