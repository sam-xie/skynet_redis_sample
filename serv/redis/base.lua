local skynet = require "skynet"


local util = require "util.common"
local hash = require "serv.redis.ds.hash"
local set = require "serv.redis.ds.set"
local zset = require "serv.redis.ds.zset"
local cjson = require "cjson"
local baseDao = class()

local PRINT_PREFIX = '[redis] '

local function printTraceback(msg)
    skynet.error(debug.traceback(msg, 2))
end


local hackmeta = {}

function hackmeta.__index(t, k)
	local cmd = string.upper(k)
	return function(self, v, ...)
		if type(self[2]) ~= 'table' then
			skynet.error(PRINT_PREFIX .. 'hackmeta.__index failed')
			skynet.error(util.dumpObj(self))
		end
		table.insert(self[2], {cmd, v, ...})
		return "QUEUED"
	end
end

--[[
baseDao.define = {
	name = "test",
	key = "id",
	[auto_incr = {"id", "numberId"},]
	[auto_incr_init = {100},]
	[index = {
		fieldName1 = {set | zset | hash, params},
		fieldName2 = {set | zset | hash, params},
		...
	},]
	[inObject = true,]
--]]

local function encode_dbvalue(value)
	if value == nil then
		return nil
	end

	local t = type(value)
	if t == "string" then
		return "@"..value
	end

	assert(t == "number", PRINT_PREFIX .. 'incompatible value type :' .. type(value))
	return value
end

local function decode_dbvalue(value)
	if value == nil then
		return nil
	end

	if value:sub(1, 1) == "@" then
		return value:sub(2)
	end

	return tonumber(value)
end

function baseDao:register(conn)
	local define = self.define

	skynet.error("[redis] register dao", define.name)

	if define.inObject then
		conn:hset("name", define.name, "inObject")
		return
	end

	conn:hset("name", define.name, "normal")

	local key = table.concat({"index", define.name}, ":")
	conn:hset(key, define.key, "key")

	local index = {}
	if type(define.index) == "table" then
		for k,v in pairs(define.index) do
			if type(v) ~= "table" then
				v = { v }
			end

			local kind = v[1]
			local params
			local str = {}

			if v[2] then
				params = {}
			end

			for i,w in ipairs(v) do
				if i > 1 then
					params[w] = true
				end

				table.insert(str, w)
			end

			conn:hset(key, k, table.concat(str, "|"))
			index[k] = {kind, params}
		end
	end
	define.index = index

	local auto_incr = define.auto_incr
	if type(auto_incr) ~= "table" then
		auto_incr = { auto_incr }
	end
	define.auto_incr = auto_incr

	local auto_incr_init = define.auto_incr_init
	if type(auto_incr_init) ~= "table" then
		auto_incr_init = { auto_incr_init }
	end

	for i,v in ipairs(auto_incr_init) do
		local key = table.concat({"auto_incr", define.name, auto_incr[i]}, ":")
		local n = tonumber(conn:get(key)) or 0
		if v > n then
			conn:set(key, v)
			skynet.error(("[redis] set %s to %d"):format(key, v))
		end
	end
end

function baseDao:init(conn)
	self.conn = conn
	self.queue = nil
end

function baseDao:hash(...)
	local key = table.concat({...}, ":")
	return hash(self.conn, key)
end

function baseDao:set(...)
	local key = table.concat({...}, ":")
	return set(self.conn, key)
end

function baseDao:zset(...)
	local key = table.concat({...}, ":")
	return zset(self.conn, key)
end

function baseDao:doPipeline(queue)
	local res = self.conn:pipeline(queue)
	if res == nil then
		local errstr = {"exec failed! cmds:"}
		for i,v in ipairs(queue) do
			table.insert(errstr, table.concat(v, " "))
		end
		return false, table.concat(errstr, "\n")
	end
	return true
end

function baseDao:multi()
	local conn = self.conn
	local queue = self.queue

	assert(queue == nil, PRINT_PREFIX .. 'error base.multi calls can not be nested')
	queue = {{"multi"}}

	self.queue = queue
	self.meta = getmetatable(conn)
	rawset(conn, 2, queue)

	setmetatable(conn, hackmeta)
end

function baseDao:exec()
	local conn = self.conn
	local queue = self.queue
	local meta = self.meta

	assert(queue, PRINT_PREFIX .. 'error base.exec without base.multi')

	self.queue = nil
	self.meta = nil

	table.insert(queue, {"exec"})
	setmetatable(conn, meta)
	rawset(conn, 2, nil)
	return self:doPipeline(queue)
end

function baseDao:discard()
	local conn = self.conn
	local queue = self.queue
	local meta = self.meta

	assert(queue, PRINT_PREFIX .. 'error base.discard without base.multi')

	self.queue = nil
	self.meta = nil

	table.insert(queue, {"discard"})
	setmetatable(conn, meta)
	rawset(conn, 2, nil)
	return self:doPipeline(queue)
end

function baseDao:doMulti(func)
	self:multi()
	local ok, res = xpcall(func, printTraceback)
	if not ok then
		self:discard()
		assert(ok, PRINT_PREFIX .. 'base.doMulti failed')
	end
	return self:exec()
end

function baseDao:updateIndex_set(name, key, field, v1, v2, members)
	assert(v1 ~= nil or v2 ~= nil)
	if (v1 ~= nil and v2 ~= nil) and (members == nil or (members[v1] and members[v2])) then
		local s1 = self:set("index", name, field, v1)
		local s2 = self:set("index", name, field, v2)
		return s1:move(s2.key, key)
	end

	if v1 == nil then
		if members == nil or members[v2] then
			local s2 = self:set("index", name, field, v2)
			s2:add(key)
		end
	elseif v2 == nil then
		if members == nil or members[v1] then
			local s1 = self:set("index", name, field, v1)
			s1:rem(key)
		end
	end
end

function baseDao:updateIndex_zset(name, key, field, v1, v2)
	local z = self:zset("index", name, field)

	if v2 == nil then
		z:rem(key)
	else
		--z:add("XX", v2, key) -- Redis 3.0.2 or greater
		z:add(v2, key)
	end
end

function baseDao:updateIndex_hash(name, key, field, v1, v2)
	local h = self:hash("index", name, field)

	if v1 ~= nil then
		h:set(v1, nil)
	end

	if v2 ~= nil then
		h:setnx(v2, key)
	end
end

function baseDao:updateIndex(name, key, field, v1, v2)
	local indexdef = self.define.index[field]
	if indexdef == nil then
		return
	end

	local kind, params = table.unpack(indexdef)
	self["updateIndex_"..kind](self, name, key, field, v1, v2, params)
end

function baseDao:selectIndexKeys_set(name, field, value, members)
	local skeys = {}
	if type(value) ~= "table" then
		value = {value}
	end
	local prefix = table.concat({"index", name, field}, ":")
	for i,v in ipairs(value) do
		if members == nil or members[v] then
			table.insert(skeys, table.concat({prefix, v}, ":"))
		end
	end
	return self.conn:sunion(table.unpack(skeys))
end

function baseDao:selectIndexKeys_zset(name, field, value)
	local z = self:zset("index", name, field)
	return z:rangebyscore(value[1], value[2])
end

function baseDao:selectIndexKeys_hash(name, field, value)
	local h = self:hash("index", name, field)
	return {h:get(value)}
end

function baseDao:selectIndexKeys(name, field, value)
	local indexDef = self.define.index[field]
	if not indexDef then
		return {}
	end
	local kind, params = table.unpack(indexDef)
	return self["selectIndexKeys_"..kind](self, name, field, value, params)
end

function baseDao:selectIndexInterSetKeys(where)
	local define = self.define
	local name = define.name
	local index = define.index

	local skeys = {}
	local prefix = table.concat({"index", name}, ":")
	for k,v in pairs(where) do
		local indexDef = index[k]
		if indexDef and indexDef[1] == "set" and type(v) ~= "table" then
			table.insert(skeys, table.concat({prefix, k, v}, ":"))
			where[k] = nil
		end
	end

	if #skeys == 0 then
		return nil
	end

	return self.conn:sinter(table.unpack(skeys))
end

function baseDao:selectKeys(where)
	local define = self.define
	local name = define.name

	if where == nil or next(where) == nil then
		local s = self:set("key", name)
		return s:members()
	end

	local keyvalue = where[define.key]
	if keyvalue then
		local keys = {}
		if type(keyvalue) ~= "table" then
			keyvalue = {keyvalue}
		end

		for i,v in ipairs(keyvalue) do
			table.insert(keys, table.concat({name, v}, ":"))
		end
		return keys
	end

	local mark = {}
	local count = 0

	local interskeys = self:selectIndexInterSetKeys(where)
	if interskeys then
		for i,w in ipairs(interskeys) do
			mark[w] = (mark[w] or 0) + 1
		end
		count = count + 1
	end

	for k,v in pairs(where) do
		local keys = self:selectIndexKeys(name, k, v)
		for i,w in ipairs(keys) do
			mark[w] = (mark[w] or 0) + 1
		end
		count = count + 1
	end

	local result = {}
	for k,v in pairs(mark) do
		if v == count then
			table.insert(result, k)
		end
	end
	return result
end

function baseDao:repeatfunc(func, times)
	local errstr
	for i = 1, times or 10 do
		local ok
		ok, errstr = func()
		if ok then
			return
		end
	end

	error(PRINT_PREFIX .. "repeatfunc failed!")
end

function baseDao:fetchAutoIncrValue(row)
	local define = self.define
	local auto_incr = define.auto_incr
	local conn = self.conn

	local prefix = table.concat({"auto_incr", define.name}, ":")
	for i,key in ipairs(auto_incr) do
		local k = table.concat({prefix, key}, ":")

		local value = row[key]
		if value == nil then
			value = conn:incr(k)
			row[key] = value
		else
			self:repeatfunc(function()
				conn:watch(k)

				local cur = conn:get(k) or 0
				if tonumber(value) <= tonumber(cur) then
					conn:unwatch()
					return true
				end

				return self:doMulti(function ()
					conn:set(k, value)
					skynet.error(("[redis] set %s to %d"):format(key, value))
				end)
			end)
		end
	end
end

function baseDao:insert(row)
	local define = self.define
	assert(not define.inObject, PRINT_PREFIX .. 'base.insert only for NOT inObject')

	local name = define.name
	local conn = self.conn

	self:fetchAutoIncrValue(row)

	local value = row[define.key]
	assert(value ~= nil, PRINT_PREFIX .. 'base.insert row must contain the define.key attr')

	local h = self:hash(name, value)

	self:repeatfunc(function()
		conn:watch(h.key)

		if h:exists() then
			conn:unwatch()
			return false, (PRINT_PREFIX .. "insert failed! key already exists:"..tostring(h.key))
		end

		return self:doMulti(function()
			h:setData(row, encode_dbvalue)
			local s = self:set("key", name)
			s:add(h.key)
			for k,v in pairs(define.index) do
				self:updateIndex(name, h.key, k, nil, row[k])
			end
		end)
	end, 1)

	return row
end
baseDao.create = baseDao.insert

function baseDao:getValues(fields, keys)
	local result = {}

	for i,key in ipairs(keys) do
		local h = self:hash(key)
		local data = h:getData(fields, decode_dbvalue)
		if data then
			table.insert(result, data)
		end
	end

	return result
end

function baseDao:select(fields, where)
	local define = self.define
	assert(not define.inObject, PRINT_PREFIX .. 'base.select only for NOT inObject')

	local name = define.name
	local keys = self:selectKeys(where)

	return self:getValues(fields, keys)
end

function baseDao:update(setter, where)
	local define = self.define
	assert(not define.inObject, PRINT_PREFIX .. 'base.update only for NOT inObject')

	local name = define.name
	local conn = self.conn
	local keys = self:selectKeys(where)

	if not next(keys) then
		return false
	end

	setter[define.key] = nil

	self:repeatfunc(function()
		conn:watch(table.unpack(keys))

		local updates = {}
		for i,key in ipairs(keys) do
			local h = self:hash(key)
			local ups = {}

			for k,newv in pairs(setter) do
				if not newv then -- 'field = false' means 'field = nil'
					newv = nil
				end

				local oldv = decode_dbvalue(h:get(k))
				if oldv ~= newv then
					ups[k] = {oldv, newv}
				end
			end
			if next(ups) then
				table.insert(updates, {key = key, ups = ups})
			end
		end

		if not next(updates) then
			conn:unwatch()
			return true
		end

		return self:doMulti(function ()
			for i,v in ipairs(updates) do
				local h = self:hash(v.key)
				for k,w in pairs(v.ups) do
					h:set(k, encode_dbvalue(w[2]))
					self:updateIndex(name, v.key, k, w[1], w[2])
				end
			end
		end)
	end)

	return true
end

function baseDao:delete(where)
	local define = self.define
	assert(not define.inObject, PRINT_PREFIX .. 'base.delete only for NOT inObject')
	assert(type(where) == 'table' and (next(where) ~= nil), 'where must be a table and not empty on detele')

	local name = define.name
	local conn = self.conn
	local keys = self:selectKeys(where)

	if not next(keys) then
		return false
	end

	self:repeatfunc(function()
		conn:watch(table.unpack(keys))

		local index = define.index
		local delindex = {}
		for i,key in ipairs(keys) do
			local idx = {}
			local h = self:hash(key)
			for k,v in pairs(index) do
				local value = decode_dbvalue(h:get(k))
				if value then
					idx[k] = value
				end
			end
			delindex[key] = idx
		end

		return self:doMulti(function()
			for key,dels in pairs(delindex) do
				local h = self:hash(key)

				for k,v in pairs(dels) do
					self:updateIndex(name, key, k, v, nil)
				end

				local s = self:set("key", name)
				s:rem(key)

				h:del()
			end
		end)
	end)

	return true
end

function baseDao:count()
	local define = self.define
	assert(not define.inObject, PRINT_PREFIX .. 'base.count only for NOT inObject')

	local name = define.name
	local s = self:set("key", name)
	return s:card()
end


function baseDao:createRows(rows, requiredAttrs)
	if type(requiredAttrs) ~= "table" then
		requiredAttrs = {'id'}
	else
		table.insert(requiredAttrs, 'id')
	end

	for _,v in pairs(rows) do
		for _, attr in ipairs(requiredAttrs) do
			if not v[attr] then
				skynet.error("[redis] : invalid params on insert into `" .. self.define.name .. "`, attr `" .. attr .. "` is required")
				return false
			end
		end

		self:create(v)
	end

	return rows
end

function baseDao:updateRowsByAttr(rows, identifyAttr, exceptantAttr)
	local where = {}
	for _, v in pairs(rows) do
		if not v[identifyAttr] then
			skynet.error("[redis] : invalid params on updating `" .. self.define.name .. "`, attr `" .. identifyAttr .. "` is required")
			return false
		end

		local ida = v[identifyAttr]
		-- empty the attr which must could not be updated
		v.id = nil
		v.key = nil
		v.playerId = nil
		v.createTime = nil
		-- empty attr by exceptantAttr
		if exceptantAttr then
			for _, val in ipairs(exceptantAttr) do
				v[val] = nil
			end
		end

		where[identifyAttr] = ida
		self:update(v, where)
	end
	return true
end

-- inObject mode
function baseDao:save(rowId, row, ...)
	local define = self.define
	assert(define.inObject, PRINT_PREFIX .. 'base.save only for inObject')
	assert(rowId ~= nil, PRINT_PREFIX .. "rowId is nil")
	assert(row, PRINT_PREFIX .. 'attr for row is empty')

	local key = define.key
	local array = {}
	for _,v in ipairs{row, ...} do
		local id = v[key]
		local code = cjson.encode(v)
		assert(id)

		table.insert(array, id)
		table.insert(array, code)
	end

	local h = self:hash(define.name, rowId)
	return h:mset(table.unpack(array))
end

function baseDao:load(rowId, id, ...)
	local define = self.define
	assert(define.inObject, PRINT_PREFIX .. 'base.load only for inObject')
	assert(rowId ~= nil, "rowId is nil!")

	local h = self:hash(define.name, rowId)
	local result = h:mget(id, ...)
	local maxI = 0
	for i,v in pairs(result) do
		result[i] = cjson.decode(v)
		maxI = i > maxI and i or maxI
	end
	return table.unpack(result, 1, maxI)
end

function baseDao:loadall(rowId)
	local define = self.define
	assert(define.inObject, PRINT_PREFIX .. 'base.loadall only for inObject')
	assert(rowId ~= nil, "rowId is nil!")

	local h = self:hash(define.name, rowId)
	local all = h:getall()
	local result = {}
	for i = 2, #all, 2 do
		table.insert(result, cjson.decode(all[i]))
	end
	return result
end

function baseDao:remove(rowId, id, ...)
	local define = self.define
	assert(define.inObject, PRINT_PREFIX .. 'base.remove only for inObject')
	assert(rowId ~= nil, "rowId is nil!")

	local h = self:hash(define.name, rowId)
	return h:remove(id, ...)
end

function baseDao:removeall(rowId)
	local define = self.define
	assert(define.inObject, PRINT_PREFIX .. 'base.removeall only for inObject')
	assert(rowId ~= nil, "rowId is nil!")

	local h = self:hash(define.name, rowId)
	return h:del()
end

return baseDao
