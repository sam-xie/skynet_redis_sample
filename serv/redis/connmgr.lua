local queue = require "skynet.queue"
local redis = require "skynet.db.redis"
local env = require "serv.conf.redis"
local manager = {}

local hosts

function manager.init()
	hosts = {}
	for id,info in pairs(env) do
		local conns = {}
		
		for i = 1, info.conn do
			local obj = redis.connect(info)
			table.insert(conns, {obj, queue()})
		end

		hosts[id] = {
			conns = conns,
			index = 0,
		}
	end
end

function manager.selectConn(id)
	assert(hosts)

	if id == nil then
		id = "main"
	end

	local status = hosts[id]
	local conns = status.conns
	local index = status.index

	local idx = (index % #conns) + 1 
	status.index = idx

	return table.unpack(conns[idx])
end

return manager
