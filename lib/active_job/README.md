# ActiveJob Adapter for Rescheduler

## How to use it

-   In your Rails application, under `config/initializers` directory, create a .rb file for active_job configuration, for example, active_job.rb. Put this line in the file:
   ```ruby
   Rails.application.config.active_job.queue_adapter = :rescheduler
   ```

-   Then you are good to go.

## A Simple Example

Suppose you want to start a job, say `dummy_job`. All you need to do is:
-   Create a ActiveJob class for it.
-   Create a rescheduler for all the active job queues.


In dummy_job.rb:

```ruby
class DummyJob < ApplicationJob
  queue_as :dummy_queue

  def perform(data)
    puts data
  end
end
```

In dummy_worker.rb

```ruby
Rescheduler.add_active_jobs('dummy_queue')
Rescheduler.start
```

1. #####You can start the worker by running shell command:
```bash
rails runner dummy_worker.rb
```

2. #####In your rails application, nothing need to be changed, you can enqueue job like:
```ruby
DummyJob.perform_later({name: 'My Name', age: 100})
DummyJob.set(wait: 10.minutes).perform_later(User.last)
```