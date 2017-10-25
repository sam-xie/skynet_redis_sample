local base = class()

function base:init(conn, key)
	self.conn = conn
	self.key = key
end

function base:del()
	local conn = self.conn
	local key = self.key

	return conn:del(key)
end

function base:exists(...)
	local conn = self.conn
	local key = self.key

	return conn:exists(key, ...)
end

return base
