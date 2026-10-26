-- Reserve tokens from a tenant's per minute budget before calling a model.
-- KEYS[1] bucket hash (fields: tokens, ts)
-- ARGV[1] capacity (tokens per minute), ARGV[2] cost
-- Returns { allowed (1/0), tokens left (string), retry after in ms }
--
-- Refill is continuous at capacity / 60 s. Time comes from Redis TIME so every
-- gateway instance refills the bucket at the same rate.
local capacity = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local per_ms = capacity / 60000

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * per_ms)

local allowed, retry = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) / per_ms)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return { allowed, tostring(tokens), retry }
