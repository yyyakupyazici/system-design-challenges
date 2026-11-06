-- Koşullu artış: "limitin altındaysa artır". Tek bir Redis komutu bunu yapamaz (INCR koşulsuz artırır),
-- GET + INCR'yi iki ayrı çağrı yapmak ise yarış durumu yaratır. Lua: oku-karar ver-yaz tek atomik adım.
-- KEYS[1] sayaç   ARGV[1] limit   Dönüş: 1 artırıldı | 0 limit dolu
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
if v >= tonumber(ARGV[1]) then
  return 0
end
redis.call('INCR', KEYS[1])
return 1
