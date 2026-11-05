--[[
  Token/dakika limiti (001'deki token bucket, "1 istek" yerine "n token" düşer).
  KEYS: 1 kova   ARGV: 1 dakikadaki token kapasitesi, 2 n (>0 al, <0 iade), 3 TTL ms
  Dönüş: {1, 0} | {0, bekleme_ms}
]]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local cap = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local rate = cap / 60000                     -- ms başına token

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil then tokens = cap; ts = now end
tokens = math.min(cap, tokens + math.max(0, now - ts) * rate)

local allowed = 1
local wait = 0
if n < 0 then
  tokens = math.min(cap, tokens - n)          -- iade (tahmin gerçekten fazlaydı)
elseif tokens >= n then
  tokens = tokens - n
else
  allowed = 0
  wait = math.ceil((n - tokens) / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
return { allowed, wait }
