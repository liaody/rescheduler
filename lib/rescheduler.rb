require 'date'
require 'time' # Needed for Time.parse
require 'multi_json'
require 'redis'

require File.expand_path('../rescheduler/worker', __FILE__)

=begin

Immediate Queue:  "TMTUBE:queue"  - List of qnids
Deferred Tasks:   "TMDEFERRED"    - Sorted set of all tasks based on their due date
Task Args:        "TMARGS:qnid"   - JSON args of the task
Running Tasks:    "TMRUNNING"     - ??

Maintenane:       "TMMAINT"       - ?? An internal queue for maintenance jobs

Worker Semaphore: "TMWORKERLOCK"  - Exclusion semaphore for worker maintenance
Auto-increment Id:"TMCOUNTER"     - Global unique id generator

Worker Registry:  "TMWORKERS"     - Map of workerid=>worker info
=end

# NOTE: We use class variables instead of class instance variables so that
# "include Rescheduler" would work as intended for DSL definition


module Rescheduler
  extend self

  # Setup configuration
  def config
    @@config ||= { prefix:'' }
    @@config
  end

  def config=(c); @@config = c; end

  #====================================================================
  # Global management / Query
  #====================================================================
  def prefix
    return @@config[:prefix]
  end

  def reinitialize
    keys = %w{TMCOUNTER TMMAINT TMDEFERRED TMARGS TMRUNNING}.map {|p| prefix + p }
    %w{TMTUBE:* TMARGS:*}.each do |p|
      keys += redis.keys(prefix + p)
    end
    redis.del(keys)
  end

  # Warning: Linear time operation (see #show_queue)
  def delete_queue(queue)
    entries = show_queue(queue)
    return 0 if entries.blank?

    entries.map do |entry|
      idelete(get_qnid(queue, entry))
    end.length
  end

  def fast_delete_immediate_queue(queue) # NOTE: only use this when there is no inserters around
    argkeys = redis.keys(rk_args(get_qnid(queue, '*')))
    redis.multi do
      redis.del(argkeys)
      redis.del(rk_queue(queue))
    end
    nil
  end

  # Return a hash of statistics
  def stats
    stats = {}
    @@runners ||= {}
    @@runners.keys.each {|queue| stats[queue] = {} } unless @@runners.blank?

    # Discover all immediate queues
    ql = rk_queue('').length
    redis.keys(rk_queue('*')).each do |rkqueue|
      queue = rkqueue[ql..-1]
      stats[queue] ||= {}
      stats[queue][:immediate] = queue_length(queue)
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
    return {:jobs=>stats, :workers=>Worker.stats}
  end

  #----------------------------------------------
  # Queue management
  #----------------------------------------------
  # Returns number of jobs waiting to be handled in a queue (all immediate jobs)
  def queue_length(queue)
    return redis.llen(rk_queue(queue))
  end

  # Reads a background job and returns its properties; returns nil if the job does not exist
  # Takes :queue and :id as arguments
  def peek(options)
    qnid = get_qnid(options[:queue], options[:id])
    optstr = redis.get(rk_args(qnid))
    return nil unless optstr
    sopt = MultiJson.load(optstr, :symbolize_keys => true)
    sopt[:queue] = options[:queue]
    sopt[:id] = options[:id]
    return sopt
  end

  # Warning: Linear time operation, where n is the number if items in all the queues
  def show_queue(queue)
    qstr = ":#{queue}:"
    # TODO: Use SCAN after upgrade to Redis 2.8
    redis.keys(rk_args(get_qnid(queue, '*'))).map {|k| k.split(qstr, 2).last }
  end

  #----------------------------------------------
  # Task management
  #----------------------------------------------

  # Check existence of one task
  def exists?(options)
    raise ArgumentError, 'Can not test existence without :id' unless options.include?(:id)
    qnid = get_qnid(options[:queue], options[:id])
    return redis.exists(rk_args(qnid))
  end

  # Delete one task
  def delete(options)
    qnid = get_qnid(options[:queue], options[:id])
    idelete(qnid)
  end

  #====================================================================
  # Task producer routines
  #====================================================================
  # Add an immediate task to the queue
  def enqueue(options=nil)
    internal_enqueue(options, false)
  end

  def enqueue_to_top(options = nil)
    internal_enqueue(options, true)
  end

  def internal_enqueue(options, push_to_top)
    sopt = options ? options.dup : {}
    queue = sopt[:queue] || '' # Default queue name is ''
    has_id = sopt.include?(:id)
    job_id = sopt[:id] || redis.incr(rk_counter) # Default random unique id

    now = Time.now.to_i

    # Error check
    validate_queue_name(queue)
    validate_recurrance(sopt)

    # Convert due_in to due_at
    if sopt.include?(:due_in)
      # log_debug 'Both due_in and due_at specified, favoring due_in' if sopt.include?(:due_at)
      sopt[:due_at] = now + sopt[:due_in]
    end

    qnid = get_qnid(queue, job_id)

    ts = sopt[:due_at].to_i
    if ts == 0 || ts < now # immediate
      ts = now
      sopt.delete(:due_at)
    else
      raise ArgumentError, 'Can not enqueue_to_top deferred jobs' if push_to_top
      sopt[:due_at] = ts # Convert :due_at to integer timestamp to be reused in recurrance
    end

    # Encode and save args
    redis.multi do # Transaction to enqueue the job and save args together
      if has_id # Delete possible existing job if user set id
        redis.zrem(rk_deferred, qnid)
        redis.lrem(rk_queue(queue), 0, qnid) # This is going to be slow for long queues
      end

      # Save args even if it is empty (for existence checks)
      redis.set(rk_args(qnid), MultiJson.dump(sopt))

      # Determine the due time
      if ts > now # Future job
        redis.zadd(rk_deferred, ts, qnid)
      else
        if push_to_top
          redis.rpush(rk_queue(queue), qnid)
        else
          redis.lpush(rk_queue(queue), qnid)
        end
      end
    end

    # Now decide if we need to wake up the workers (outside of the transaction)
    if (ts > now)
      dt = redis.zrange(rk_deferred, 0, 0)[0]
      # Wake up workers if our job is the first one in deferred queue, so they can reset timeout
      if dt && dt == qnid
        redis.lpush(rk_maintenace, '-') # Kick start the maintenance token ring
      end
    end
    nil
  end

  # Temp function for special purpose. Completely by-pass concurrency check to increase speed
  def quick_enqueue_batch(queue, ids, reset = false)
    argsmap = {}
    vals = []
    ids.each do |id|
      qnid = get_qnid(queue, id)
      vals << qnid
      argsmap[rk_args(qnid)] = '{}' # Empty args
    end unless ids.blank?
    redis.pipelined do # Should do redis.multi if concurrency is a problem
      redis.del(rk_queue(queue)) if reset # Empty the list fast
      unless ids.blank?
        redis.lpush(rk_queue(queue), vals)
        argsmap.each { |k,v| redis.set(k,v) }
      end
    end
    nil
  end

  # Returns true if enqueued a new job, otherwise returns false
  def enqueue_unless_exists(options)
    # NOTE: There is no point synchronizing exists and enqueue
    return false if exists?(options)
    enqueue(options)
    return true
  end

  #====================================================================
  # Job definition
  #====================================================================

  # Task consumer routines
  def job(tube, &block)
    @@runners ||= {}
    @@runners[tube] = block
    return nil
  end

  #====================================================================
  # Error handling
  #====================================================================
  def on_error(tube=nil, &block)
    if tube != nil
      @@error_handlers ||= {}
      @@error_handlers[tube] = block
    else
      @@global_error_handler = block;
    end
  end

  #====================================================================
  # Runner/Maintenance routines
  #====================================================================
  def start(*tubes)
    # Check arguments
    if !@@runners || @@runners.size == 0
      raise Exception, 'Can not start worker without defining job handlers.'
    end

    tubes.each do |t|
      next if @@runners.include?(t)
      raise Exception, "Handler for queue #{t} is undefined."
    end

    tubes = @@runners.keys if !tubes || tubes.size == 0

    log_debug "[[ Starting: #{tubes.join(',')} ]]"

    # Generate a random clientkey for maintenance token ring maintenance.
    client_key = [[rand(0xFFFFFFFF)].pack('L')].pack('m0')[0...6]

    keys = tubes.map {|t| rk_queue(t)}
    keys << rk_maintenace

    # Queue to control a named worker
    worker_queue = Worker.rk_queue if Worker.named?
    keys.unshift(worker_queue) if worker_queue # worker control queue is the first we respond to

    dopush = nil

    @@end_job_loop = false
    while !@@end_job_loop
      # Run maintenance and determine timeout
      next_job_time = determine_next_deferred_job_time.to_i

      if dopush # Only pass-on the token after we are done with maintenance. Avoid contention
        redis.lpush(rk_maintenace, dopush)
        dopush = nil
      end

      # Blocking wait
      timeout = next_job_time - Time.now.to_i
      timeout = 1 if timeout < 1

      # A producer may insert another job after BRPOP and before WATCH
      # Due to limitations of BRPOP we can not prevent this from happening.
      # When it happens we will consume the args of the later job, causing
      # the newly inserted job to be "promoted" to the front of the queue
      # This may not be desirable...
      # (too bad BRPOPLPUSH does not support multiple queues...)
      # TODO: Maybe LUA script is the way out of this.
      result = redis.brpop(keys, :timeout=>timeout)

      # Handle task
      if result # Got a task
        tube = result[0]
        qnid = result[1]
        if tube == rk_maintenace
          # Circulate maintenance task until it comes a full circle. This depends on redis
          # first come first serve policy in brpop.
          dopush = qnid + client_key unless qnid.include?(client_key) # Push if we have not pushed yet.
        elsif tube == worker_queue
          Worker.handle_command(qnid)
        else
          run_job(qnid)
        end
      else
        # Do nothing when got timeout, the run_maintenance will take care of deferred jobs
      end
    end
  end

  def end_job_loop; @@end_job_loop = true; end


  # Logging facility
  def log_debug(msg)
    return if config[:silent]
    print("#{Time.now.iso8601} #{msg}\n")
    STDOUT.flush
  end

  #====================================================================
  private
  #====================================================================

  # Internal routines operating out of qnid
  def idelete(qnid)
    queue = qnid.split(':').first
    redis.multi do
      redis.del(rk_args(qnid))
      redis.zrem(rk_deferred, qnid)
      redis.lrem(rk_queue(queue), 0, qnid)
    end
  end

  # Runner routines
  def run_job(qnid)
    # 1. load job parameters for running
    optstr = nil
    key = rk_args(qnid)
    # Atomic get and delete the arg
    redis.multi do
      optstr = redis.get(key)
      redis.del(key)
    end

    optstr = optstr.value # get the value from the multi block future
    if optstr.nil?
      log_debug("Job is deleted mysteriously: (#{qnid})")
      return # Job is deleted somewhere
    end

    # Parse and run
    sopt = MultiJson.load(optstr, :symbolize_keys => true)
    queue,id = qnid.split(':', 2)
    sopt[:queue] ||= queue
    sopt[:id] ||= id

    # Handle parameters
    if (sopt.include?(:recur_every))
      newopt = sopt.dup
      newopt[:due_at] = (sopt[:due_at] || Time.now).to_i + sopt[:recur_every].to_i
      newopt.delete(:due_in) # In case the first job was specified by :due_in
      log_debug("---Enqueue #{qnid}: recur_every #{sopt[:recur_every]}")
      enqueue(newopt)
    end

    if (sopt.include?(:recur_daily) || sopt.include?(:recur_weekly))
      newopt = sopt.dup
      newopt.delete(:due_at)
      newopt.delete(:due_in) # No more due info, just the recurrance
      log_debug("---Enqueue #{qnid}: recur_daily #{sopt[:recur_daily]}") if sopt.include?(:recur_daily)
      log_debug("---Enqueue #{qnid}: recur_weekly #{sopt[:recur_weekly]}") if sopt.include?(:recur_weekly)
      enqueue(newopt)
    end

    # 2. Find runner and invoke it
    begin
      log_debug(">>---- Starting #{qnid}")
      runner = @@runners[qnid_to_queue(qnid)]
      if runner.is_a?(Proc)
        runner.call(sopt)
        log_debug("----<< Finished #{qnid}")
        Worker.inc_job_count # Stats for the worker
      else
        log_debug("----<< Failed #{qnid}: Unknown queue name, handler not defined")
      end
    rescue Exception => e
      log_debug("----<< Failed #{qnid}: -------------\n #{$!}")
      log_debug(e.backtrace[0..4].join("\n"))
      handle_error(e, queue, sopt)
      log_debug("------------------------------------\n")
    end
  end

  def handle_error(e, queue, sopt)
    @@error_handlers ||= {}
    @@global_error_handler ||= nil
    error_handler = @@error_handlers[queue]
    if error_handler
      error_handler.call(e, sopt)
    elsif @@global_error_handler
      @@global_error_handler.call(e, sopt)
    end
  end

  # Helper routines

  # Find all the "due" deferred jobs and move them into respective queues
  def service_deferred_jobs
    dtn = rk_deferred # Make a copy in case prefix changes
    ntry = 0
    while ntry < 6 do
      curtime = Time.now.to_i
      return if redis.zcount(dtn, 0, curtime) == 0

      limit = ntry < 3 ? 100 : 1 # After first 3 tries, do just 1
      redis.watch(dtn) do
        tasks = redis.zrangebyscore(dtn, 0, curtime, :limit=>[0,limit]) # Serve at most 100
        if tasks.empty?
          redis.unwatch
          return # Nothing to transfer, moving on.
        end

        to_push = {}
        tasks.each do |qnid|
          q = rk_queue(qnid_to_queue(qnid))
          to_push[q] ||= []
          to_push[q] << qnid
        end

        redis.multi
        redis.zrem(dtn, tasks)

        to_push.each do |q, qnids|
          redis.lpush(q, qnids) # Batch command
        end

        if !redis.exec
          # Contention happens, retrying
          # Sleep a random amount of time after first try
          ntry += 1
          log_debug("service_deferred_jobs(#{limit}) contention")
          Kernel.sleep (rand(ntry * 1000) / 1000.0)
        else
          return # Done transfering
        end
      end
    end

    log_debug("service_deferred_jobs failed, will try next time")
  end

  def determine_next_deferred_job_time(skip_service = nil)
    tsnow = Time.now.to_f
    maxtime = tsnow + 3600 + rand(100) # Randomize wake time to avoid multi worker service contention

    dt = redis.zrange(rk_deferred, 0, 0, :with_scores=>true)[0]
    nt = (dt && dt[1] && dt[1] < maxtime) ? dt[1] : maxtime
    if !skip_service && nt <= tsnow
      do_if_can_acquire_semaphore do
        service_deferred_jobs
        # Get the deferred jobs again.
        dt = redis.zrange(rk_deferred, 0, 0, :with_scores=>true)[0]
        nt = (dt && dt[1] && dt[1] < maxtime) ? dt[1] : maxtime
      end
    end
    return nt
  end

  def rk_queue(queue); "#{prefix}TMTUBE:#{queue}"; end

  def rk_deferred; prefix + 'TMDEFERRED'; end
  def rk_maintenace; prefix + 'TMMAINT'; end
  def rk_args(qnid); "#{prefix}TMARGS:#{qnid}"; end
  def rk_counter; prefix + 'TMCOUNTER'; end
  def rk_worker_semaphore; prefix + 'TMWORKERLOCK'; end # This is a boolean with a timeout for workers to exclude each other

  # None blocking, returns true if semaphore is acquired (for a given timeout), this is cooperative to avoid guaranteed contentions
  def try_acquire_semaphore(timeout=300) # Default for 5 minutes, there must be a timeout
    semkey = rk_worker_semaphore
    if redis.setnx(semkey, 1) # Any value would be fine
      redis.expire(semkey, timeout)
      return true
    else
      # Already created, someone has it
      return false
    end
  end

  # This releases semaphore unconditionally
  def release_semaphore
    # NOTE: There is a chance we remove the lock created by another worker after our own expired.
    # This is OK since the lock is cooperative and not necessary (real locking is done through contention checks)
    redis.del(rk_worker_semaphore)
  end

  # Run block only if the semaphore can be acquired, otherwise do nothing
  def do_if_can_acquire_semaphore
    if try_acquire_semaphore
      yield
      release_semaphore
    end
  end

  def get_qnid(queue, id); return "#{queue}:#{id}"; end

  def qnid_to_queue(qnid)
    idx = qnid.index(':')
    unless idx
      log_debug("Invalid qnid: #{qnid}")
      return nil
    end
    qnid[0...idx]
  end

  def redis
    @@redis ||= config[:redis] || Redis.new(config[:redis_connection] || {})
  end

  def validate_queue_name(queue)
    raise ArgumentError, 'Queue name can not contain special characters' if queue.include?(':')
  end

  # Find the next recur time
  def time_from_recur_daily(recur_daily, now = Time.now)
    parsed = Date._parse(recur_daily)
    if !parsed[:hour] || (parsed.keys - [:zone, :hour, :min, :sec, :offset, :sec_fraction]).present?
      raise ArgumentError, 'Unexpected recur_daily value: ' + recur_daily
    end

    if !parsed[:offset]
      raise ArgumentError, 'A timezone is required for recur_daily: ' + recur_daily
    end

    # Never offset over one day (e.g. 23:59 PDT)
    offset = (parsed[:hour] * 3600 + (parsed[:min]||0) * 60 + (parsed[:sec] || 0) - parsed[:offset]) % 86400

    t = Time.utc(now.year, now.month, now.day) + offset
    t += 86400 if t <= now + 1
    return t
  end

  def time_from_recur_weekly(recur_weekly, now = Time.now)
    parsed = Date._parse(recur_weekly)
    if !parsed[:hour] || !parsed[:wday] || (parsed.keys - [:wday, :zone, :hour, :min, :sec, :offset, :sec_fraction]).present?
      raise ArgumentError, 'Unexpected recur_weekly value: ' + recur_weekly
    end

    if !parsed[:offset]
      raise ArgumentError, 'A timezone is required for recur_weekly: ' + recur_weekly
    end

    # Never offset over one week
    offset = parsed[:hour] * 3600 + (parsed[:min]||0) * 60 + (parsed[:sec] || 0) - parsed[:offset]
    offset = (offset + parsed[:wday] * 86400) % (86400 * 7)

    t = Time.utc(now.year, now.month, now.day) - now.wday * 86400 + offset
    t += 86400 * 7 if t <= now + 1
    return t
  end

  def validate_recurrance(options)
    rcnt = 0
    if (options.include?(:recur_every))
      rcnt += 1
      raise 'Expect integer for :recur_every parameter' unless options[:recur_every].is_a?(Fixnum)
    end

    if (options.include?(:recur_daily))
      rcnt += 1
      time = time_from_recur_daily(options[:recur_daily]) # Try parse and make sure we can
      unless options.include?(:due_at) || options.include?(:due_in)
        options[:due_at] = time # Setup the first run
      end
    end

    if (options.include?(:recur_weekly))
      rcnt += 1
      time = time_from_recur_weekly(options[:recur_weekly]) # Try parse and make sure we can
      unless options.include?(:due_at) || options.include?(:due_in)
        options[:due_at] = time # Setup the first run
      end
    end

    raise 'Can only specify one recurrance parameter' if rcnt > 1
  end

end
