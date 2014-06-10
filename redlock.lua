-- 
-- Redis Distributed Locks implementation for TurboRedis.
--
-- Port of redlock-rb (https://github.com/antirez/redlock-rb)
--
-- Read http://redis.io/topics/distlock before using.
--
local turbo = require("turbo")
local turboredis = require("turboredis")
local task = turbo.async.task
local yield = coroutine.yield

redlock = {}

redlock.UNLOCK_SCRIPT = [[
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
]]

redlock.CLOCK_DRIFT_FACTOR = 0.01

redlock.DEFAULT_RETRY_COUNT = 3

redlock.DEFAULT_RETRY_DELAY = 200

redlock.urandom = io.open("/dev/urandom")

-- yield this to 'sleep asynchronously' for `msecs` milliseconds
function redlock.turbosleep(msecs)
    local ioloop = turbo.ioloop.instance()
    return task(ioloop.add_timeout, ioloop,
        turbo.util.gettimemonotonic() + msecs)
end

redlock.LockManager = class("LockManager")
function redlock.LockManager:initialize(servers, opts)
    opts = opts or {}
    self.servers = {}
    for _,server in ipairs(servers) do
        self.servers[#self.servers+1] = turboredis.Connection:new(server.host, 
            server.port, server.opts)
    end
    if #self.servers == 1 then
        self.quorum = 1
    else
        self.quorum = (#self.servers / 2) + 1 
    end
    self.retry_count = opts.retry_count or redlock.DEFAULT_RETRY_COUNT
    self.clock_drift_factor = opts.clock_drift_factor or redlock.CLOCK_DRIFT_FACTOR
    self.retry_delay = opts.retry_delay or redlock.DEFAULT_RETRY_DELAY
    math.randomseed(turbo.util.gettimemonotonic())
    self.urandom = io.open("/dev/urandom")
end

function redlock.LockManager:get_unique_lock_id()
    return (string.gsub(self.urandom:read(32), "(.)", function (c)
        return string.format("%02X", string.byte(c))
    end))
end

function redlock.LockManager:_connect(callback, callback_arg)
    turbo.ioloop.instance():add_callback(function () 
        local failed = false
        for i, redis in ipairs(self.servers) do
            local r = yield(redis:connect())
            if not r then
                turbo.log.error(string.format("Could not connect to %s:%d",
                    redis.host, redis.port))
                failed = true
                break
            end
        end
        if callback_arg then
            callback(callback_arg, not failed)
        else
            callback(not failed)
        end
    end)
end

function redlock.LockManager:_lock_instance(redis, resource, val, ttl)
    return redis:set(resource, val, "NX", "PX", ttl)
end

function redlock.LockManager:_unlock_instance(redis, resource, val)
    return redis:eval(redlock.UNLOCK_SCRIPT, 1, resource, val)
end

function redlock.LockManager:_lock(resource, ttl, callback, callback_arg)
    turbo.ioloop.instance():add_callback(function ()
        local val = self:get_unique_lock_id()
        local tries = 0
        repeat
            local n = 0
            local start_time = turbo.util.gettimemonotonic()
            for _, redis in ipairs(self.servers) do
                local r = yield(self:_lock_instance(redis, resource, val, ttl))
                if r then
                    n = n + 1
                end
            end
            local drift = math.floor(ttl*redlock.CLOCK_DRIFT_FACTOR) + 2
            local validity_time = ttl-(turbo.util.gettimemonotonic() - start_time)-drift
            if n >= self.quorum and validity_time > 0 then
                local lock ={
                    validity=validity_time,
                    resource=resource,
                    val=val
                }
                if callback_arg then
                    callback(callback_arg, lock)
                else
                    callback(lock)
                end
                return
            else
                for _,redis in ipairs(self.servers) do
                    yield(self:_unlock_instance(redis, resource, val))
                end
            end
            yield(redlock.turbosleep(math.random(self.retry_delay)))
            tries = tries + 1
        until tries == self.retry_count
        if callback_arg then
            callback(callback_arg, false)
        else
            callback(false)
        end
    end)
end

function redlock.LockManager:_unlock(lock, callback, callback_arg)
    turbo.ioloop.instance():add_callback(function ()
        for i, redis in ipairs(self.servers) do
            yield(self:_unlock_instance(redis, lock.resource, lock.val))
        end
        if callback_arg then
            callback(callback_arg, true)
        else
            callback(true)
        end
    end)
end

function redlock.LockManager:connect(callback, callback_arg)
    if callback then
        return self:_connect(callback, callback_arg)
    else
        return task(self._connect, self)
    end
end

function redlock.LockManager:lock(resource, ttl, callback, callback_arg)
    if callback then
        return self:_lock(resource, ttl, callback, callback_arg)
    else
        return task(self._lock, self, resource, ttl)
    end
end

function redlock.LockManager:unlock(lock, callback, callback_arg)
    if callback then
        return self:_unlock(lock, callback, callback_arg)
    else
        return task(self._unlock, self, lock)
    end
end

return redlock
