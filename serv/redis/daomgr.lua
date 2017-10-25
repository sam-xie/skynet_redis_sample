local skynet = require "skynet"
local dc = require "skynet.datacenter"
local socket_error = require "skynet.socketchannel".error
local lfs = require "lfs"
local connmgr = require "serv.redis.connmgr"
local util = require "util.common"
local CMD = {}
local daoCls = {}

_G.class = require "util.class"
_G.baseDao = require "serv.redis.base" -- preload

local INIT_SLEEP_TIME = 10
local SLEEP_TIME_STEP_LEN = 20
local MAX_SLEEP_TIME = 500
local sleepTime = INIT_SLEEP_TIME

function CMD.launchDao(path)
	skynet.error("[redis] daomgr.launchDao", path)

	local rootPath = dc.get("path", "root")
	local iter, dirObjs = lfs.dir(rootPath .. path)

	local requirePathPrefix = string.gsub(path, '/', '.') .. '.'
	while true do
		local fileName = iter(dirObjs)
		if not fileName then
			break
		end

		local daoName = string.match(fileName, "(%w+)dao.lua$")
		if daoName then
			local cls = require(requirePathPrefix..daoName.."dao")
			local define = cls.define
			local conn = connmgr.selectConn(define.host)

			cls:register(conn)
			daoCls[daoName] = cls
		end
	end
end

local function doQuery(funcName, func, obj, ...)
	while true do
		local n = select("#", ...)
		local params = {}
		for i,v in pairs{...} do
			params[i] = util.copy(v)
		end
		local ret = {xpcall(func, debug.traceback, obj, table.unpack(params, 1, n))}
		local ok = table.remove(ret, 1)
		if ok then
			sleepTime = INIT_SLEEP_TIME
			return table.unpack(ret)
		end

		if ret[1] ~= socket_error then
			error(ret[1]) -- throw
		end

		skynet.error("[redis] doQuery socket error! retry ...")
		skynet.sleep(sleepTime)
		sleepTime = sleepTime + SLEEP_TIME_STEP_LEN
		sleepTime = sleepTime > MAX_SLEEP_TIME and MAX_SLEEP_TIME or sleepTime
	end
end

function CMD.query(daoName, funcName, ...)
	local cls = daoCls[daoName]
	if not cls then
		skynet.error("[redis] Can't find dao", daoName)
		return
	end

	local define = cls.define
	local conn, cs = connmgr.selectConn(define.host)
	local obj = cls(conn)
	local func = assert(obj[funcName], string.format("[redis] Unknown funcName('%s') in dao('%s') ", funcName, daoName))

	return cs(doQuery, funcName, func, obj, ...)
end

local function doRespond(cmds, status, arg1, ...)
	if status then
		skynet.ret(skynet.pack(arg1, ...))
	else
		error("[redis] Error: " .. arg1 .. '\n[redis] query params:\n' .. util.dumpObj(cmds))
	end
end

local function onRequest(session, source, cmd, ...)
	assert(session > 0, "[redis] Use skynet.call!")
	local f = assert(CMD[cmd], "[redis] Unknown cmd: "..tostring(cmd))
	doRespond({cmd, ...}, xpcall(f, debug.traceback, ...))
end


skynet.start(function()
	skynet.dispatch("lua", onRequest)
	dc.set("serv", "redis", skynet.self())
	connmgr.init()
end)
