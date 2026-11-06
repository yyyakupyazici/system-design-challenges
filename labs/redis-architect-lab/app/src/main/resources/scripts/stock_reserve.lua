-- Flash sale stok rezervasyonu: "stok yetiyorsa düş ve siparişi kaydet", idempotent.
-- KEYS[1] stock:{sku}   KEYS[2] stock:{sku}:orders (sipariş → adet)   — aynı hash tag
-- ARGV[1] sipariş id   ARGV[2] adet
-- Dönüş: 1 rezerve edildi | 0 stok yetmedi | 2 bu sipariş zaten rezerve (retry)
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return 2
end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
local qty = tonumber(ARGV[2])
if stock < qty then
  return 0
end
redis.call('DECRBY', KEYS[1], qty)
redis.call('HSET', KEYS[2], ARGV[1], qty)
return 1
