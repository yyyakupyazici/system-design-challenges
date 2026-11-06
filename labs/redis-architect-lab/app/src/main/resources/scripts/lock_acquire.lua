-- Kilit al + fencing token üret, tek atomik adımda.
-- KEYS[1] kilit (lock:{kaynak})   KEYS[2] fencing sayacı (lock:{kaynak}:fence) — aynı hash tag, aynı slot
-- ARGV[1] sahip token'ı (UUID)    ARGV[2] süre (ms)
-- Dönüş: fencing token (> 0) | 0 kilit başkasında
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
