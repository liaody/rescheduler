require 'time' # Needed for Time.parse
require 'json'
require 'redis'

module Rescheduler
  extend self

  # Setup configuration
  attr_accessor :config
  self.config = {}

  # Debugging only (since initializers won't reload)
  self.config[:prefix] = REDIS_PREFIX # This ensures cross DB deployment persistence
  self.config[:redis] = RedisHelper.redis

  #==========================
  # Management routines
  def prefix
    return @config[:prefix] || ''
  end

  def reinitialize # Very slow reinitialize
    keys = %w{TMCOUNTER TMMAINT TMDEFERRED TMARGS TMRUNNING}.map {|p| prefix + p }
    %w{TMTUBE:*}.each do |p|
      keys += redis.keys(prefix + p)
    end
    redis.del(keys)
  end

  # Return a hash of statistics, in this format
  # 
  def stats
    loop do 
      redis.watch(rk_args) do
        stats = {}
        qnids = redis.hkeys(rk_args)
        # Get all the "pending jobs"
        qnids.each do |qnid|
          queue = qnid_to_queue(qnid)
          stats[queue] ||= {}
          stats[queue][:pending] ||= 0
          stats[queue][:pending] += 1
        end

        # Get all running
        qnids = redis.hkeys(rk_running)
        # Get all the "pending jobs"
        qnids.each do |qnid|
          queue = qnid_to_queue(qnid)
          stats[queue] ||= {}
          stats[queue][:running] ||= 0
          stats[queue][:running] += 1
        end

        # Get all the deferred
        deferred = redis.zrange(rk_deferred, 0, -1, :with_scores=>true)
        deferred.each do |qnid, ts|
          queue = qnid_to_queue(qnid)
          stats[queue] ||= {}
          stats[queue][:deferred] ||= 0
          stats[queue][:deferred] += 1
          stats[queue][:first] ||= ts # First is first
        end        

        # Get all the immediate
        qus = stats.keys
        quls = redis.multi do
          qus.each { |queue| redis.llen(rk_queue(queue)) }
        end

        unless quls # Retry
          log_debug('Contention during stats')
          next 
        end

        qus.each_with_index do |k, idx|
          stats[k][:immediate] = quls[idx]
        end

        return {:jobs=>stats}
      end
    end
  end

  #==========================
  # Task producer routines
  # Add an immediate task to the queue
  def enqueue(options=nil)
    options ||= {}
    now = Time.now

    # Error check 
    validate_queue_name(options[:queue]) if options.include?(:queue)
    validate_recurrance(options)

    # Convert due_in to due_at
    if options.include?(:due_in)
      raise ArgumentError, ':due_in and :due_at can not be both specified' if options.include?(:due_at)
      options[:due_at] = now + options[:due_in]
    end

    # Get an ID if not already have one     
    user_id = options.include?(:id)
    unless user_id
      options[:id] = redis.incr(rk_counter)
    end

    ts = options[:due_at].to_i || 0
    qnid = get_qnid(options)

    # Encode and save args
    redis.multi do # Transaction to enqueue the job and save args together
      if user_id # Delete possible existing job if user set id
        redis.zrem(rk_deferred, qnid)
        redis.lrem(rk_queue(options[:queue]), 0, qnid)
      end

      # Save options
      redis.hset(rk_args, qnid, options.to_json)

      # Determine the due time
      if ts > now.to_i # Future job
        redis.zadd(rk_deferred, ts, qnid)
      else
        redis.lpush(rk_queue(options[:queue]), qnid)
      end
    end

    # Now decide if we need to wake up the workers (outside of the transaction)
    if (ts > now.to_i)
      dt = redis.zrange(rk_deferred, 0, 0)[0]
      # Wake up workers if our job is the first one in deferred queue, so they can reset timeout
      if dt && dt == qnid
        redis.lpush(rk_maintenace, '-') # Kick start the maintenance token ring
      end
    end
    nil
  end

  def exists?(options)
    raise ArgumentError, 'Can not test existence without :id' unless options.include?(:id)
    qnid = get_qnid(options)
    return redis.hexists(rk_args, qnid)
  end

  def enqueue_unless_exists(options)
    enqueue(options) unless exists?(options)
  end

  def delete(options)
    qnid = get_qnid(options)
    
    redis.multi do
      redis.hdel(rk_args, qnid)
      redis.zrem(rk_deferred, qnid)
      redis.lrem(rk_queue(options[:queue]), 0, qnid)
    end
  end

  # Make a job immediate if it is not already. Erase the wait
  def make_immediate(options)
    dtn = rk_deferred # Make a copy in case prefix changes
    qnid = get_qnid(options)
    ntry = 0
    loop do
      redis.watch(dtn) do 
        if redis.zcard(dtn, qnid) == 0
          redis.unwatch(dtn)
          return # Not a deferred job
        else
          redis.multi
          redis.zrem(dtn, qnid)
          q = qnid_to_queue(qnid)
          redis.lpush(rk_queue(q), qnid)
          if !redis.exec
            # Contention happens, retrying
            log_debug("make_immediate contention for #{qnid}")
            Kernel.sleep (rand(ntry * 1000) / 1000.0) if ntry > 0
          else
            return # Done
          end
        end
      end
      ntry += 1
    end
  end

  #=================
  # Serialization (in case it is needed to transfer all Rescheduler across to another redis instance)

  # Atomically save the state to file and stop all workers (state in redis is not destroyed)
  # This function can take a while as it will wait for running jobs to finish first.
  def serialize_and_stop(filename)
    # TODO
  end

  # Load state from a file. Will merge into existing jobs if there are any (make sure it is done only once)
  # This can be done before any worker starts, or after. 
  # Workers still need to be manually started
  def deserialize(filename)
    # TODO
  end

  # Clear redis states and delete all jobs (useful before deserialize)
  def erase_all
    # TODO
  end

  #=================
  # Job definition
  # Task consumer routines
  def job(tube, &block)
    @runners ||= {}
    @runners[tube] = block
    return nil
  end

  #=================
  # Error handling
  def on_error(tube=nil, &block)
    if tube != nil
      @error_handlers ||= {}
      @error_handlers[tube] = block
    else
      @global_error_handler = block;
    end
  end
  #=================
  # Runner/Maintenance routines
  def start(*tubes)
    # Check arguments
    if !@runners || @runners.size == 0
      raise Exception, 'Can not start worker without defining job handlers.'
    end

    tubes.each do |t|
      next if @runners.include?(t)
      raise Exception, "Handler for queue #{t} is undefined."
    end

    tubes = @runners.keys if !tubes || tubes.size == 0

    log_debug "[[ Starting: #{tubes.join(',')} ]]"

    # Generate a random clientkey for maintenance token ring maintenance.
    client_key = [[rand(0xFFFFFFFF)].pack('L')].pack('m0')[0...6]

    keys = tubes.map {|t| rk_queue(t)}
    keys << rk_maintenace

    dopush = nil

    loop do
      # Run maintenance and determine timeout
      next_job_time = determine_next_deferred_job_time.to_i

      if dopush # Only pass-on the token after we are done with maintenance. Avoid contention
        redis.lpush(rk_maintenace, dopush) 
        dopush = nil
      end

      # Blocking wait
      timeout = next_job_time - Time.now.to_i
      timeout = 1 if timeout < 1
      result = redis.brpop(keys, :timeout=>timeout)

      # Handle task
      if result # Got a task
        tube = result[0]
        qnid = result[1]
        if tube == rk_maintenace
          # Circulate maintenance task until it comes a full circle. This depends on redis 
          # first come first serve policy in brpop. 
          dopush = qnid + client_key unless qnid.include?(client_key) # Push if we have not pushed yet.
        else
          run_job(qnid)
        end
      else 
        # Do nothing when got timeout, the run_maintenance will take care of deferred jobs
      end
    end
  end

  private 

  # Runner routines
  def run_job(qnid)
    # 1. load job parameters for running
    optstr = nil
    begin
      res = nil
      # Note: We use a single key to watch, can be improved by having a per-job key, 
      redis.watch(rk_args) do # Transaction to ensure read/delete is atomic
        optstr = redis.hget(rk_args, qnid)        
        if optstr.nil?
          redis.unwatch
          log_debug("Job is deleted mysteriously")
          return # Job is deleted somewhere
        end
        res = redis.multi do 
          redis.hdel(rk_args, qnid)
          redis.hset(rk_running, qnid, optstr)
        end
        if !res
          # Contention, try read again
          log_debug("Job read contention: (#{qnid})")
        end
      end
    end until res

    # Parse and run
    opt = JSON.parse(optstr)
    #opt.symbolize_keys # Be mindful of non-rails people, explicitly here
    sopt = {}
    opt.each { |key, val| sopt[key.to_sym] = val }

    # Handle parameters
    if (sopt.include?(:due_every))
      newopt = sopt.dup
      newopt[:due_at] = (sopt[:due_at] || Time.now) + sopt[:due_every]
      newopt.delete(:due_in) # In case the first job was specified by :due_in
      enqueue(newopt)
    end

    if (sopt.include?(:recur_daily))
      newopt = sopt.dup      
      newopt[:due_at] = time_from_recur_daily(sopt[:recur_daily])
      newopt.delete(:due_in) # In case the first job was specified by :due_in
      enqueue(newopt)
    end

    # 2. Find runner and invoke it
    begin
      log_debug(">>---- Starting #{qnid}")
      runner = @runners[qnid_to_queue(qnid)]
      if runner.is_a?(Proc)
        runner.call(sopt)
        log_debug("----<< Finished #{qnid}")
      else
        log_debug("----<< Failed #{qnid}: Unknown queue name, handler not defined")
      end
    rescue Exception => e
      log_debug("----<< Failed #{qnid}: -------------\n #{$!}")
      log_debug(e.backtrace[0..4].join("\n"))
      handle_error(e, qnid, sopt)
      log_debug("------------------------------------\n")
    end

    # 3. Remove job from running list (Done)
    redis.hdel(rk_running, qnid)
  end

  def handle_error(e, qnid, sopt)
    error_handler = @error_handlers && @error_handlers[qnid]
    if error_handler
      error_handler.call(e, sopt) 
    elsif @global_error_handler
      @global_error_handler.call(e, sopt) 
    end
  end

  # Helper routines

  # Find all the "due" deferred jobs and move them into respective queues
  def service_deferred_jobs
    dtn = rk_deferred # Make a copy in case prefix changes
    ntry = 0
    loop do
      curtime = Time.now.to_i
      redis.watch(dtn) do
        tasks = redis.zrangebyscore(dtn, 0, curtime)
        if tasks.empty?
          redis.unwatch
          return # Nothing to transfer, moving on.
        end

        redis.multi
        redis.zremrangebyscore(dtn, 0, curtime)
        tasks.each do |qnid|
          q = qnid_to_queue(qnid)
          redis.lpush(rk_queue(q), qnid)
        end
        if !redis.exec
          # Contention happens, retrying
          # Sleep a random amount of time after first try
          log_debug("service_deferred_jobs contention")
          Kernel.sleep (rand(ntry * 1000) / 1000.0) if ntry > 0
        else
          return # Done transfering
        end
      end
      ntry += 1
    end
  end

  def determine_next_deferred_job_time(skip_service = nil)
    tsnow = Time.now.to_f
    maxtime = tsnow + 3600

    dt = redis.zrange(rk_deferred, 0, 0, :with_scores=>true)[0]
    nt = (dt && dt[1] && dt[1] < maxtime) ? dt[1] : maxtime
    if !skip_service && nt <= tsnow
      service_deferred_jobs
      # Get the deferred jobs again.
      dt = redis.zrange(rk_deferred, 0, 0, :with_scores=>true)[0]
      nt = (dt && dt[1] && dt[1] < maxtime) ? dt[1] : maxtime
    end
    return nt
  end

  def rk_queue(queue); "#{prefix}TMTUBE:#{queue}"; end

  def rk_deferred; prefix + 'TMDEFERRED'; end
  def rk_maintenace; prefix + 'TMMAINT'; end
  def rk_args; prefix + "TMARGS"; end
  def rk_running; prefix + "TMRUNNING"; end
  def rk_counter; prefix + 'TMCOUNTER'; end

  def get_qnid(options)
    return "#{options[:queue]}:#{options[:id]}"
  end

  def qnid_to_queue(qnid)
    idx = qnid.index(':')
    unless idx
      log_debug("Invalid qnid: #{qnid}")
      return nil 
    end
    qnid[0...idx]
  end

  def redis
    @redis ||= @config[:redis] || Redis.new(@config[:redis_connection] || {})
  end

  def validate_queue_name(queue)
    raise ArgumentError, 'Queue name can not contain special characters' if queue.include?(':')
  end

  def parse_seconds_of_day(recur_daily)
    return recur_daily if recur_daily.is_a?(Fixnum)
    time = Time.parse(recur_daily)
    return time.to_i - Time.new(time.year, time.month, time.day).to_i
  end

  # Find the next recur time
  def time_from_recur_daily(recur_daily, now = Time.now)
    recur = parse_seconds_of_day(recur_daily)
    t = Time.new(now.year, now.month, now.day).to_i + recur
    t += 86400 if t < now.to_i
    return Time.at(t)
  end

  def validate_recurrance(options)
    rcnt = 0
    if (options.include?(:due_every))
      rcnt += 1
      raise 'Expect integer for :due_every parameter' unless options[:due_every].is_a?(Fixnum)
    end

    if (options.include?(:recur_daily))
      rcnt += 1
      time = time_from_recur_daily(options[:recur_daily]) # Try parse and make sure we can
      unless options.include?(:due_at) || options.include?(:due_in)
        options[:due_at] = time # Setup the first run
      end
    end
    raise 'Can only specify one recurrance parameter' if rcnt > 1
  end

  # Logging facility
  def log_debug(msg)
    return if @config[:silent]
    print("#{Time.now.iso8601} #{msg}\n")
    STDOUT.flush
  end

end
