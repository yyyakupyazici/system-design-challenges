-- Gecikmeli iş planla: içerik hash'e, zaman ZSET'e. İkisi birlikte yazılmalı (yarım iş kalmasın).
-- KEYS[1] dq:{kuyruk}:ready   KEYS[2] dq:{kuyruk}:jobs   ARGV[1] iş id   ARGV[2] payload   ARGV[3] çalışma zamanı (ms)
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
