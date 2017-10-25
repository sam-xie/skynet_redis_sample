local base = require((...):match("(.-)[^%.]+$") .. 'base')
local hash = class(base)

function hash:set(field, value)
	local conn = self.conn
	local key = self.key

	if value == nil then
		return conn:hdel(key, field)
	end
	return conn:hset(key, field, value)
end

function hash:setnx(field, value)
	local conn = self.conn
	local key = self.key

	return conn:hsetnx(key, field, value)
end

function hash:get(field)
	local conn = self.conn
	local key = self.key

	return conn:hget(key, field)
end

function hash:mset(field1, value1, ...)
	local conn = self.conn
	local key = self.key

	return conn:hmset(key, field1, value1, ...)
end

function hash:mget(field1, ...)
	local conn = self.conn
	local key = self.key

	return conn:hmget(key, field1, ...)
end

function hash:getall()
	local conn = self.conn
	local key = self.key

	return conn:hgetall(key)
end

function hash:remove(field1, ...)
	local conn = self.conn
	local key = self.key

	return conn:hdel(key, field1, ...)
end

function hash:setData(data, encoder)
	local conn = self.conn
	local key = self.key

	encoder = encoder or function(...) return ... end

	local args = {}
	for k,v in pairs(data) do
		table.insert(args, k)
		table.insert(args, encoder(v))
	end
	conn:hmset(key, table.unpack(args))
	return true
end

function hash:getData(fields, decoder)
	local conn = self.conn
	local key = self.key

	decoder = decoder or function(...) return ... end

	local data = {}
	if fields then
		local list = conn:hmget(key, table.unpack(fields))
		for i,v in ipairs(fields) do
			data[v] = decoder(list[i])
		end
	else
		local all = conn:hgetall(key)
		for i = 1, #all, 2 do
			data[all[i]] = decoder(all[i + 1])
		end
	end

	if not next(data) then
		return nil
	end
	return data
end

return hash
