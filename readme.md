# Rescheduler

## Example (pure ruby)
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

## Example (using rescheduler_launch)
In your jobs.rb
```ruby
job('ring-alarm') do |arg|
  Music.find_by_name(arg[:song]).play
end

start # You need to start your job explicitly
```

Then launch it
```
rescheduler_launch worker% jobs.rb
```

## Installation
```
  gem install rescheduler
```
## Dependency

* Redis 2.4 or above
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
1. Priority of jobs
2. Edit pending job parameter without changing its position in queue
3. Producer throttling (max queue length)
4. Programmable queue priority (handle the tube with most job first?)
5. Class.perform style of worker definition(?)
6. Other clients (Java etc.)
7. Detection and recovery of dead workers
8. Detection and recovery of "lost" jobs
9. Allow cherry-picking of jobs from special clients

### Non-goals

### Known bugs/deficiencies
* Long immediate queues with unique id: Scanning the queue to delete possible duplicated job can be slow
* Fast deferred job producers: When deferred job is produced very fast (more than 5/sec) the service of deferred jobs may not find a chance to move any deferred job to immediate queue
* Job jumping: If job(id:A) is freshly dequeued by a worker while a producer tries to generate the same job with id:A, there is a slight chance that new job parameter will be fetched by the worker that tries to run the old job.

## API
Please read the source (not ready for production use yet)

### Configuration
```ruby
Rescheduler.config[:redis_config] # Redis configuration hash (see https://github.com/redis/redis-rb)
Rescheduler.config[:prefix] = '' # Redis key prefix, for namespacing
```

NOTE: Change prefix will invalidate current jobs.

### Other similar tools
* Resque: Uses polling
* qless: Uses LUA scripts inside redis server


## Worker Documentations

### Options
* --rails, --rails=<dir> Specify the rails environment to load before using Rescheduler
    This is useful for fetching the exact Rails setup for Reschduler for worker processes
    This will also set the log output to the correct place