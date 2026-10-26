-- Settle a reservation once the real usage is known.
-- KEYS[1] bucket hash, ARGV[1] capacity, ARGV[2] delta
-- delta > 0 refunds an overestimate. delta < 0 charges an underestimate, and may
-- push the balance below zero: the tenant is then in debt and waits for refill.
local capacity = tonumber(ARGV[1])
local delta = tonumber(ARGV[2])
local per_ms = capacity / 60000

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * per_ms + delta)

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return tostring(tokens)
