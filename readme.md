# Rescheduler

## Example
In your jobs.rb

```ruby 
require 'rescheduler'

Rescheduler.job('ring-alerm') do |arg|
  Music.find_by_name(arg[:song]).play
end

Rescheduler.start # This starts the worker infinite loop
```

To schedule the job
```ruby
Rescheduler.enqueue(:queue=>'ring-alerm', :song=>'Jingle', :due_in => 600) # Use 10.minutes if using active support
```

## Installation
```
  gem install rescheduler
```
## Dependency
  
* Redis 2.4
* Redis ruby client (https://github.com/redis/redis-rb)
* JSON

## Features
1. Based solely on Redis. Can configure connection, db, prefix.
2. Immediate response time for immediate jobs. (Event machine style, no polling)
3. CPU idle when there is no job. (Block waiting, no polling)
4. Low setup. Auto maintained by workers. 

Other Features

5. "id" supports querying, overwriting and removing of pending jobs.

## Design Goals

### Motivations

1. Simplify Deployment: Redis as the only in-memory cache infrastructure piece.
2. Simplify Persistence: backup and replication can be shared with all Redis data sets.
3. Fast response: Beanstalkd's no polling pipe architecture for immediate job response time.
4. No Setup: Can not require extra "maintenance thread" in background. Has to be self maintained.

### TODO later
1. Priority of tasks
2. Edit pending task parameter without changing its position in queue
3. Producer throttling (max queue length)
4. Programmable priority (handle the tube with most task first?)
5. Class.perform style of runner definition(?)
6. Other clients (Java etc.)
7. Detection and recovery of dead workers

### Non-goals


## API
enqueue(options)

Options:
:due_at => time
:due_in => number of seconds from now
:queue => Name of the queue (default if missing)
:id => Uniquely identify the job within its queue. (Overwrites job with same id)
[anything else] => passed over to runner

delete(options)
:queue (optional)
:id    (required)

exists?(options)
:queue
:id


### Configuration

Rescheduler.config[:redis_config] # Redis configuration hash (see https://github.com/redis/redis-rb)

TimeMachine.config[:prefix] = '' # Redis key prefix
NOTE: Change prefix will invalidate current jobs.

## Internals
* One LIST for each queue for immediate jobs
* One global "deferred" sorted set, score is timestamp
* One global "counter" integer for auto-incremented IDs
* One global "maintenance" list, special queue for maintenance tasks

## Other similar jobs
* Resque: Uses polling
* qless: Uses LUA scripts and depends on Redis 2.6. 
