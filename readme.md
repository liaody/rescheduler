# Rescheduler

## Example
In your jobs.rb

```ruby 
require 'rescheduler'

Rescheduler.job('ring-alarm') do |arg|
  Music.find_by_name(arg[:song]).play
end

Rescheduler.start # This starts the worker infinite loop
```

To schedule the job
```ruby
Rescheduler.enqueue(:queue=>'ring-alarm', :song=>'Jingle', :due_in => 600) # Use 10.minutes if using active support
```

## Installation
```
  gem install rescheduler
```
## Dependency
  
* Redis 2.4
* Redis ruby client (https://github.com/redis/redis-rb)
* MultiJson (https://github.com/intridea/multi_json)

## Features
1. Based solely on Redis. Can configure connection, db, prefix.
2. Immediate response time for immediate jobs. (Event machine style, no polling)
3. CPU idle when there is no job. (Block waiting, no polling)
4. Light weight and low setup. Queues automatically maintained by workers. 
5. "id" supports querying, overwriting and removing of pending jobs.

### TODO later
0. Testing
1. Priority of tasks
2. Edit pending task parameter without changing its position in queue
3. Producer throttling (max queue length)
4. Programmable priority (handle the tube with most task first?)
5. Class.perform style of runner definition(?)
6. Other clients (Java etc.)
7. Detection and recovery of dead workers
8. Detection and recovery of "lost" jobs
9. Allow cherry-picking of jobs from special clients

### Non-goals


## API
Please read the source (not ready for production use yet)

### Configuration

Rescheduler.config[:redis_config] # Redis configuration hash (see https://github.com/redis/redis-rb)
Rescheduler.config[:prefix] = '' # Redis key prefix
NOTE: Change prefix will invalidate current jobs.

## Internals
* One LIST for each queue for immediate jobs
* One global "deferred" sorted set, score is timestamp
* One global "counter" integer for auto-incremented IDs
* One global "maintenance" list, special queue for maintenance tasks
* One global "args" hash for deferred and immediate jobs
* One global "running" hash for currently running jobs (or jobs that worker die while running)

## Other similar tools
* Resque: Uses polling
* qless: Uses LUA scripts inside redis server
