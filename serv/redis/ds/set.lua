local base = require((...):match("(.-)[^%.]+$") .. 'base')
local set = class(base)

function set:add(...)
	local conn = self.conn
	local key = self.key

	return conn:sadd(key, ...)
end

function set:rem(...)
	local conn = self.conn
	local key = self.key

	return conn:srem(key, ...)
end

function set:move(dest, member)
	local conn = self.conn
	local key = self.key

	return conn:smove(key, dest, member)
end

function set:card()
	local conn = self.conn
	local key = self.key

	return conn:scard(key)
end

function set:ismember(member)
	local conn = self.conn
	local key = self.key

	return 1 == conn:sismember(key, member)
end

function set:members()
	local conn = self.conn
	local key = self.key

	return conn:smembers(key)
end

function set:inter(...)
	local conn = self.conn
	local key = self.key
	local otherkeys = {}

	local n = select('#', ...)
	assert(n > 0)
	for i = 1, n do
		local s = select(i, ...)
		table.insert(otherkeys, s.key)
	end

	return conn:sinter(key, table.unpack(otherkeys))
end

function set:union(...)
	local conn = self.conn
	local key = self.key
	local otherkeys = {}

	local n = select('#', ...)
	assert(n > 0)
	for i = 1, n do
		local s = select(i, ...)
		table.insert(otherkeys, s.key)
	end

	return conn:sunion(key, table.unpack(otherkeys))
end

return set
