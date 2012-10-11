require 'json'
require 'redis'

module Rescheduler
  extend self

  # Setup configuration
  attr_accessor :config
  self.config = {}
  #==========================
  # Management routines
  def prefix
    return @config[:prefix] || ''
  end

  def reinitialize # Very slow reinitialize
    keys = %w{TMCOUNTER TMMAINT TMDEFERRED}
    %w{TMTUBE:* TMARGS:* TMRUNNING:*}.each do |p|
      keys += redis.keys(prefix + p)
    end
    redis.del(keys)
  end

  # Return a hash of statistics
  def stats
    qnids = redis.keys(rk_args('*'))
    stats = {}
    qnids.each do |k|
      qnid = k.split('TMARGS:')[1]
      queue = qnid_to_queue(qnid)
      stats[queue] ||= 0
      stats[queue] += 1
    end
    return {:jobs=>stats}
  end

  #==========================
  # Task producer routines
  # Add an immediate task to the queue
  def enqueue(options=nil)
    options ||= {}
    now = Time.now

    # Error check 
    validate_queue_name(options[:queue]) if options.include?(:queue)

    # Convert due_in to due_at
    if options.include?(:due_in)
      raise ArgumentError, ':due_it and :due_at can not be both specified' if options.include?(:due_at)
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
      redis.set(rk_args(qnid), options.to_json)

      # Determine the due time
      if ts > now.to_i # Future job
        redis.zadd(rk_deferred, ts, qnid)
      else
        redis.lpush(rk_queue(options[:queue]), qnid)
      end
    end

    # Now decide if we need to wait up the workers (outside of the transaction)
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
    return redis.exists(rk_args(qnid))
  end

  def enqueue_unless_exists(options)
    enqueue(options) unless exists?(options)
  end

  def delete(options)
    qnid = get_qnid(options)
    
    redis.multi do
      redis.del(rk_args(qnid))
      redis.zrem(rk_deferred, qnid)
      redis.lrem(rk_queue(options[:queue]), 0, qnid)
    end
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
    # First load job parameters for running
    rk = rk_args(qnid)
    rkr = rk_running(qnid)
    optstr = nil
    begin
      res = nil
      redis.watch(rk) do # Transaction to ensure read/delete is atomic
        optstr = redis.get(rk)        
        if optstr.nil?
          redis.unwatch
          log_debug("Job is deleted mysteriously")
          return # Job is deleted somewhere
        end
        res = redis.multi do 
          redis.del(rk)
          redis.set(rkr, optstr)
        end
        if !res
          # Contention, try read again
          log_debug("Job read contention")
        end
      end
    end until res

    # Parse and run
    opt = JSON.parse(optstr)
    #opt.symbolize_keys # Be mindful of non-rails people, explicitly here
    sopt = {}
    opt.each { |key, val| sopt[key.to_sym] = val }

    # 2. Find runner and invoke it
    begin
      log_debug(">>---- Starting #{qnid}")
      runner = @runners[qnid_to_queue(qnid)]
      runner.call(sopt)
      log_debug("----<< Finished #{qnid}")
    rescue 
      log_debug("----<< Failed #{qnid}: #{$!}")
    end

    # 3. Remove job from running list (Done)
    redis.del(rkr)
  end

  # Helper routines

  # Find all the "due" deferred jobs and move them into respective queues
  def service_deferred_jobs
    dtn = rk_deferred
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

  def rk_queue(tube)
    return "#{prefix}TMTUBE:#{tube}"
  end

  def rk_deferred; prefix + 'TMDEFERRED'; end
  def rk_maintenace; prefix + 'TMMAINT'; end
  def rk_args(qnid); "#{prefix}TMARGS:#{qnid}"; end
  def rk_running(qnid); "#{prefix}TMRUNNING:#{qnid}"; end
  def rk_counter; prefix + 'TMCOUNTER'; end

  def get_qnid(options)
    return "#{options[:queue]}:#{options[:id]}"
  end

  def qnid_to_queue(qnid); qnid[0...qnid.index(':')]; end

  def redis
    @redis ||= Redis.new(@config[:redis_connection] || {})
  end

  def validate_queue_name(queue)
    raise ArgumentError, 'Queue name can not contain special characters' if queue.include?(':')
  end

  # Logging facility
  def log_debug(msg)
    print("#{Time.now.to_i} #{msg}\n")
  end

end
