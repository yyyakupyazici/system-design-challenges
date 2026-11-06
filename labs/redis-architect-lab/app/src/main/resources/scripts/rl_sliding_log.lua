-- Kayan pencere (log): her istek ZSET'e zaman damgasıyla yazılır, pencere dışı olanlar silinir.
-- Saat Redis'ten alınır (TIME): pod'ların saati birbirinden farklı olsa da karar tutarlı kalır.
-- KEYS[1] rl:sw:{istemci}   ARGV[1] limit   ARGV[2] pencere (ms)   ARGV[3] benzersiz istek id'si
-- Dönüş: {izin 1|0, penceredeki istek sayısı}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])
if count >= tonumber(ARGV[1]) then
  return {0, count}
end
redis.call('ZADD', KEYS[1], now, ARGV[3])
redis.call('PEXPIRE', KEYS[1], window)
return {1, count + 1}
