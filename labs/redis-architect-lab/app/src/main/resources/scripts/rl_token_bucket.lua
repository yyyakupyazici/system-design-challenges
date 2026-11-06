-- Token bucket: kova saniyede `rate` jeton dolar, en fazla `capacity` jeton tutar. Patlamalara (burst)
-- capacity kadar izin verir, uzun vadede ortalamayı rate'te tutar. (001'deki limiter'ın çekirdeği)
-- KEYS[1] rl:tb:{istemci}   ARGV[1] capacity   ARGV[2] rate (jeton/sn)   ARGV[3] istenen jeton
-- Dönüş: {izin 1|0, kalan jeton (tamsayı)}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2]) / 1000
local want = tonumber(ARGV[3])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or capacity
local ts = tonumber(b[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local allowed = 0
if tokens >= want then
  tokens = tokens - want
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate) + 1000)
return {allowed, math.floor(tokens)}
