-- Sabit pencere: INCR + ilk istekte EXPIRE. İkisi ayrı çağrı olursa INCR'den sonra süreç ölünce
-- key süresiz kalır ve istemci sonsuza kadar engellenir. Bu yüzden ikisi tek script'te.
-- KEYS[1] rl:fw:{istemci}:<pencere>   ARGV[1] limit   ARGV[2] pencere (ms)
-- Dönüş: {izin 1|0, sayaç}
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if n > tonumber(ARGV[1]) then
  return {0, n}
end
return {1, n}
