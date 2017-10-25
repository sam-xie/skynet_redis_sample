local tconcat = table.concat
local tinsert = table.insert
local tostring = tostring
local pairs = pairs

local util = {}

local getIndent = function(level)
    return string.rep("\t", level)
end
local quoteStr = function(str)
    return '"' .. string.gsub(str, '"', '\\"') .. '"'
end
local wrapKey = function(val)
    if type(val) == "number" then
        return "[" .. val .. "]"
    elseif type(val) == "string" then
        return "[" .. quoteStr(val) .. "]"
    else
        return "[" .. tostring(val) .. "]"
    end
end
local wrapVal = function(val, level, limit)
    if type(val) == "table" then
        if level == limit then
            return tostring(val)
        end
        return util.dumpObj(val, level, limit)
    elseif type(val) == "number" then
        return val
    elseif type(val) == "string" then
        return quoteStr(val)
    else
        return tostring(val)
    end
end

local doDumpObj = function (obj, level, limit)
    if level == nil then
        level = 0
    end
    if limit == nil or limit < level then
        -- 9999 use as infinity
        limit = 9999
    end
    if type(obj) ~= "table" or (level == limit)  then
        return wrapVal(obj, level, limit)
    end
    level = level + 1
    local tokens = {}
    tokens[#tokens + 1] = "{"
    for k, v in pairs(obj) do
        tokens[#tokens + 1] = getIndent(level) .. wrapKey(k) .. " = " .. wrapVal(v, level, limit) .. ","
    end
    tokens[#tokens + 1] = getIndent(level - 1) .. "}"
    return tokens
end
local dumpObj = function(obj, level, limit)
    local tokens = doDumpObj(obj, level, limit)
    if type(tokens) ~= 'table' then
        return tokens
    end
    return table.concat(tokens, "\n")
end
util.dumpObj = dumpObj
local dumpObjOneLine = function(obj, level, limit)
    local tokens = doDumpObj(obj, level, limit)
    if type(tokens) ~= 'table' then
        return tokens
    end
    local ret = table.concat(tokens, '')
    return (ret:gsub('\t', ''))
end
util.dumpObjOneLine = dumpObjOneLine

function util.print_r(obj, str)
    print("print_r began =====================" .. (str or ""))
    print(dumpObj(obj, 0))
    print("print_r ended =====================")
end

local function __deepCopy(object, lookupTable)
    if type(object) ~= "table" then
        return object
    elseif lookupTable[object] then
        return lookupTable[object]
    end
    local new_table = {}
    lookupTable[object] = new_table
    for index, value in pairs(object) do
        new_table[__deepCopy(index, lookupTable)] = __deepCopy(value, lookupTable)
    end
    return setmetatable(new_table, getmetatable(object))
end
function util.deepCopy(object)
    local lookupTable = {}
    return __deepCopy(object, lookupTable)
end

local function __copy(object)
    if type(object) ~= "table" then
        return object
    end
    local newTable = {}
    for i, v in pairs(object) do
        newTable[i] = __copy(v)
    end
    return newTable
end
function util.copy(object)
    return __copy(object)
end

return util
