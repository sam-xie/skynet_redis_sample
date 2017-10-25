local skynet = require "skynet"
local dc = require "skynet.datacenter"
local rdi = require "serv.redis.interface"
local util = require "util.common"

local function newPlayerDate(userId)
	assert(type(userId) == 'string' and #userId > 0, 'invalid userId')
	return {
		id = "user_" .. userId,
		userId = userId,
		name = "",
		lv = 1,
		-- other else, such as createTime
	}
end

local function newItemData(playerId, key, count)
	return {
		playerId = playerId,
		key = key,
		count = count,
		-- other else
	}
end

local function transDb2DataPlayer(dbres) return dbres end
local function transData2DbPlayer(data) return data end

-- 'where' should include attributes which is the key or index of dao
local function getPlayer(where, fields)
	return transDb2DataPlayer(rdi.player:select(fields, where))
end

local function createPlayer(userId)
    local player = getPlayer({userId = userId})
    -- if player  is existed
    if player then
    	return player
    end

    local player = newPlayerDate(userId)
    local ok = rdi.player:create(player)
    if not ok then
    	return nil
    end
	return player
end

local function updatePlayer(player, playerId)
	playerId = playerId or player.id
	local where = {id = playerId}
    return rdi.player:update(transData2DbPlayer(player), where)
end


-- the different between creatItems and updateItems:
--   createItems assign the default attribute values to item data

local function createItems(playerId, items)
    for k, v in ipairs(items) do
        items[k] = newItemData(playerId, v.key, v.count)
    end
    rdi.item:save(playerId, table.unpack(items))
	return true, items
end

local function updateItems(playerId, items)
    return rdi.item:save(playerId, table.unpack(items))
end

local function getItems(playerId)
    assert(playerId, "getItems error: playerId must not nil")
    return rdi.item:loadall(playerId)
end

local function sndump(obj)
	skynet.error(util.dumpObj(obj))
end

skynet.start(function()
	local rootPath = skynet.getenv('root')
	dc.set("path", "root", rootPath)

	local daomgr = skynet.uniqueservice "serv/redis/daomgr"
	skynet.call(daomgr, "lua", "launchDao", "example/dao/redis")

	local result = createPlayer('userIdxxx')
	skynet.error('result from createPlayer')
	sndump(result)

	local player = result[1]
	player.name = "" .. os.time()
	updatePlayer(player)
	skynet.error('player after updatePlayer')
	sndump(getPlayer({id = player.id}))

	local items = {
		{key = 'gold', count = 100},
		{key = 'coin', count = 200},
	}
	createItems(player.id, items)
	skynet.error('items after createItems')
	sndump(getItems(player.id))

	items[1].count = 300
	updateItems(player.id, items)

	skynet.error('items after updateItems')
	sndump(getItems(player.id))

	skynet.exit()
end)