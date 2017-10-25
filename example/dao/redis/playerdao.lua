local dao = class(baseDao)

dao.define = {
	name = "player",
	key = "id",
	auto_incr = "numberId",
	auto_incr_init = 100000,
	index = {
		numberId = "hash",
		name = "set",
		userId = "set",
		lv = "zset",
	},
}

function dao:getLvById(id)
	local h = self:hash("player", id)
	if not h:exists() then
		return nil
	end
	return h:get("lv")
end

return dao
