local base = require((...):match("(.-)[^%.]+$") .. 'base')
local zset = class(base)

function zset:add(score, member, ...)
	local conn = self.conn
	local key = self.key

	return conn:zadd(key, score, member, ...)
end

function zset:rem(...)
	local conn = self.conn
	local key = self.key

	return conn:zrem(key, ...)
end

function zset:score(member)
	local conn = self.conn
	local key = self.key

	return conn:zscore(key, member)
end

function zset:range(start, stop, ...)
	local conn = self.conn
	local key = self.key

	return conn:zrange(key, start, stop, ...)
end

function zset:revrange(start, stop, ...)
	local conn = self.conn
	local key = self.key

	return conn:zrevrange(key, start, stop, ...)
end

function zset:rangebyscore(min, max, ...)
	local conn = self.conn
	local key = self.key

	return conn:zrangebyscore(key, min, max, ...)
end

function zset:revrangebyscore(max, min, ...)
	local conn = self.conn
	local key = self.key

	return conn:zrevrangebyscore(key, max, min, ...)
end

return zset
