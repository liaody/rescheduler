require 'socket'

module Rescheduler
  module Worker
    extend self
    # Each worker is a process of it own, handled by this singleton module

    NONCMD_OPTS = %w[pid machine worker_name worker_file]
    CMD_OPTS = %w[rails log env respawn] # respanw is for internal only
    attr_accessor :launch_options
    def worker_name; @launch_options && @launch_options['worker_name']; end
    def worker_index; @launch_options && @launch_options['worker_index'] || -1; end
    # ====================================================================
    # Controller
    # ====================================================================
    WORKERCMD_SUSPEND = 'suspend'
    WORKERCMD_RESUME = 'resume'
    WORKERCMD_STOP = 'stop'
    WORKERCMD_RESTART = 'restart'
    [WORKERCMD_SUSPEND, WORKERCMD_RESUME, WORKERCMD_STOP, WORKERCMD_RESTART].each do |cmd|
      define_method(cmd) {|name| redis.lpush(rk_queue(name), cmd) }
      define_method(cmd + '_all') {|pattern| cmd_to_pattern(pattern, cmd) }
    end

    def workers_from_pattern(pattern)
      pattern = pattern.gsub('%', '*')
      workers = redis.keys(rk_worker(pattern))
      kl = rk_worker('').length
      workers.map {|w| w[kl..-1]}
    end

    def cmd_to_pattern(pattern, cmd)
      ws = []
      workers_from_pattern(pattern).each do |wname|
        redis.lpush(rk_queue(wname), cmd)
        ws << wname
      end
      return ws
    end

    def kill_all(pattern, force=false)
      workers_from_pattern(pattern).each do |wname|
        kill(wname, force)
      end
    end

    # Do not run this unless everything else fails, it kills unconditionally
    def kill(worker_name, force=false)
      key = rk_worker(worker_name)
      opts = redis.hgetall(key)
      if force || opts['machine'] == Socket.gethostname
        stop(worker_name)
        sleep 1 # Give the process a second to quick peacefully
        redis.del(key)
        # Actually kill the process
        begin
          pid = opts['pid']
          # "HUP" does not get recognized in windows
          Process.kill(9, pid.to_i) if pid && opts['machine'] == Socket.gethostname
        rescue Errno::ESRCH, Errno::EPERM # No such process, Not permitted, these will be ginored
        end
      end
    end

    def clean_dead_workers
      hostname = Socket.gethostname # Only can cleanup PIDs on own machine
      workers = redis.keys(rk_worker('*'))
      workers.each do |w|
        opts = redis.hgetall(w)
        next unless opts['machine'] == hostname
        next if pid_exists?(opts['pid'])
        print "Cleaning dead worker: #{w.split(':',3).last}\n"
        redis.del(w) # Remove the record (Main part of the cleanup)
      end
      nil
    end

    def restart_self
      Rescheduler.log_debug "[#{worker_name}] Restarting"
      @respawn = true
      Rescheduler.end_job_loop
      @in_suspend = false
    end

    def handle_command(command)
      case command
      when WORKERCMD_SUSPEND
        Rescheduler.log_debug "[#{worker_name}] Suspending"
        @in_suspend = true
        suspend_loop
      when WORKERCMD_RESUME
        if @in_suspend
          Rescheduler.log_debug "[#{worker_name}] Resuming"
          @in_suspend = false # This will resume the normal job loop
        else
          Rescheduler.log_debug "[#{worker_name}] Resume command received when not suspended"
        end
      when WORKERCMD_RESTART
        restart_self
      when WORKERCMD_STOP
        Rescheduler.log_debug "[#{worker_name}] Stopping"
        Rescheduler.end_job_loop
        @in_suspend = false
      else
        Rescheduler.log_debug "[#{worker_name}] Unknown command: #{command}"
      end
    end

    def suspend_loop
      while @in_suspend do
        result = redis.brpop(rk_queue)
        if result
          command = result[1]
          handle_command(command)
        end
      end
    end

    # ====================================================================
    # Runner
    # ====================================================================

    DEVNULL = '/dev/null'
    def redirect_logging(opts)
      return if windows_env? # Do not redirect in windows environment, let it run with cmd console

      logfile = opts['log']
      logfile ||= File.join(opts['rails'], "log/#{worker_name}.log") if opts['rails']
      logfile ||= DEVNULL

      # Redirect io
      unless logfile == DEVNULL
        # Code inspired by Daemonize::redirect_io
        begin
          STDOUT.reopen logfile, "a"
          File.chmod(0644, logfile)
          STDOUT.sync = true
        rescue ::Exception
          begin; STDOUT.reopen DEVNULL; rescue ::Exception; end
        end
        begin; STDERR.reopen STDOUT; rescue ::Exception; end
        STDERR.sync = true
      end
    end

    def is_respawning?; @launch_options && @launch_options['respawn']; end

    def register(opt)
      @launch_options = opt # Save for respawning
      if @launch_options['respawn']
        wname = @launch_options['worker_name']
        # Reset created for respawn purposes.
        redis.hset(rk_worker(wname), 'created', Time.now.to_i)
      else
        name_pattern = @launch_options['worker_name']

        widx = 0 # We do not preserve worker_index upon respawn
        wname = nil
        last_wname = nil
        loop do
          wname = name_pattern.gsub('%', widx.to_s)
          if wname == last_wname # For the name without %, we add one to the end if clashes
            wname += widx.to_s
            name_pattern += '%'
          end

          break if redis.hsetnx(rk_worker(wname), 'created', Time.now.to_i)
          widx += 1
          last_wname = wname
        end
        @launch_options['worker_name'] = wname # Save this in launch options
        @launch_options['worker_index'] = widx # Sequence of the same worker
      end
      @launch_options['pid'] = Process.pid
      @launch_options['machine'] = Socket.gethostname

      # Setup the worker stats
      redis.multi do
        redis.hincrby(rk_worker, 'spawn_count', 1)
        redis.hset(rk_worker, 'job_count', 0) # Reset the job count since respawn
        redis.mapped_hmset(rk_worker, @launch_options) # Save launch options
        redis.del(rk_queue) # Clear old control commands
      end
      nil
    end

    def unregister
      redis.del(rk_worker)
    end

    def exists?(name)
      redis.hexists(rk_workers, name)
    end

    def rk_worker(name = nil)
      name ||= worker_name
      Rescheduler.prefix + 'TMWORKER:' + name
    end

    def rk_queue(name = nil)
      name ||= worker_name
      Rescheduler.prefix + 'TMWORKERQUEUE:' + name
    end

    def stats
      stats = {}
      workers = redis.keys(rk_worker('*'))
      kl = rk_worker('').length
      workers.each do |w|
        stats[w[kl..-1]] = redis.hgetall(w)
      end
      return stats
    end

    def named?
      @launch_options && @launch_options.include?('worker_name')
    end

    def inc_job_count
      # Only do this if we are launched as a worker
      if named?
        jc = redis.hincrby(rk_worker, 'job_count', 1)
        # Check for respawn_jobs
        respawn_jobs = @launch_options['respawn_jobs'].to_i
        restart_self if respawn_jobs > 0 && jc >= respawn_jobs
        # Check for respawn_time
        respawn_time = @launch_options['respawn_time'].to_i
        if respawn_time > 0
          created = redis.hget(rk_worker, 'created')
          restart_self if created && created.to_i + respawn_time < Time.now.to_i
        end
      end
    end

    def respawn_if_requested
      return false unless @respawn
      @launch_options['respawn'] = true
      spawn(@launch_options)
      return true
    end

    def spawn(opts)
      system_options = {}
      env = load_env(opts['env']) if opts.include?('env')
      env ||= {}
      #system_options['chdir'] = opts['chdir'] if opts.include?('chdir')

      cmd = "rescheduler_launch #{opt_to_str(opts)}"
      print "EXEC: #{cmd}\n"
      pid = Kernel.spawn(env, cmd, system_options)
      Process.detach(pid)
    end

    def load_env(env)
      env.is_a?(String) ? Hash[env.split(';').map{|e| a,b=e.split('=', 2); b ||= true; [a,b]}] : env
    end

    def pack_env(env)
      return env if env.is_a?(String)
      env.map {|k,v| "#{k}=#{v}"}.join(';')
    end

    def opt_to_str(opts)
      name = opts['worker_name']
      file = opts['worker_file']
      args = opts.map do |k,v|
        next if NONCMD_OPTS.include?(k)
        if k == 'env'
          "--env=#{pack_env(v)}"
        elsif v==true || v == 'true'
          "--#{k}"
        else
          "--#{k}=#{v}"
        end
      end
      args << name
      args << file
      return args.join(' ')
    end

    def redis
      Rescheduler.send :redis # Call private method in Rescheduler module
    end

    def windows_env?; RUBY_PLATFORM.end_with?('mingw32'); end
    def pid_exists?(pid)
      Process.kill(0, pid.to_i)
      return true
    rescue Errno::ESRCH
      return false
    end
  end
end
