-- Uzun süren iş için kilidin süresini uzat (sadece hâlâ sahibiysek).
-- KEYS[1] kilit   ARGV[1] sahip token'ı   ARGV[2] yeni süre (ms)   Dönüş: 1 | 0
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
