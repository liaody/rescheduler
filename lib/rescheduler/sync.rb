module Rescheduler
  module Sync
    extend self

    def lock(name, opts={})
      raise "Requires a block to be supplied" unless block_given? # Maybe later offer naked lock/unlocks?
      raise "Need a valid string name" unless name.is_a?(String)

      res = do_lock(name, opts) # This would block
      return nil unless res # Timeout or failed somehow

      begin
        yield
      ensure
        do_unlock(name)
      end
      return true # Lock was successful
    end

    # Forcefully unlock and delete existing locks
    def clear!(name)
      redis.del(rk_exists_name(name), rk_lock_name(name))
    end

    private def do_lock(name, opts)
      # Make sure semaphore for a given name is only created once
      if redis.getset(rk_exists_name(name), 1)
        # Already created, block wait for the release (possibility for unlock)
        return redis.brpop(rk_lock_name(name), timeout: (opts[:timeout] || 0))
      else
        # First time, get the lock automatically
        return true
      end
    end

    private def do_unlock(name)
      redis.lpush(rk_lock_name(name), 1)
    end

    private def redis; Rescheduler.send(:redis); end
    private def rk_exists_name(name); return "#{Rescheduler.prefix}Sync:#{name}"; end
    private def rk_lock_name(name); return "#{Rescheduler.prefix}SyncQ:#{name}"; end
  end
end