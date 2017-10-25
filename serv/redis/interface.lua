local skynet = require "skynet"
local dc = require "skynet.datacenter"
local interface = {}

local redisserv
local function getRedisServ()
	if redisserv == nil then
		redisserv = dc.get("serv", "redis")
	end
	return redisserv
end

function interface.query(daoName, funcName, ...)
	return skynet.call(getRedisServ(), "lua", "query", daoName, funcName, ...)
end

-- Usage: interface.test:update(1, 2)
return setmetatable(interface, {__index = function(t, k)
	local obj = {}
	obj.daoName = k
	rawset(interface, k, obj)

	return setmetatable(obj, {__index = function(t, k)
		return function(obj, ...)
			return interface.query(obj.daoName, k, ...)
		end
	end})
end})
