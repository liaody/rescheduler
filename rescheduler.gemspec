
Gem::Specification.new do |s|
  s.name        = 'rescheduler'
  s.version     = '0.2.0'
  s.date        = '2012-10-01'
  s.summary     = "A job queue for immediate and delayed jobs using Redis"
  s.description = <<EOD
  Rescheduler is a library that uses Redis to maintain a task queue of immediate and delayed jobs without any polling.

  Goals:
  1. Light weight. Leave as much control to user as possible.
  2. Fast response: no polling, pipe architecture for immediate job response time.
  3. No Setup: Can not require extra "maintenance thread" in background. Auto maintained by each and every worker thread.

EOD
  s.authors     = ["Dongyi Liao"]
  s.email       = 'liaody@gmail.com'
  s.files       = ["lib/rescheduler.rb"]
  s.homepage    = 'https://github.com/liaody/rescheduler'
  s.license     = 'BSD'

  s.add_runtime_dependency 'redis'
  s.add_runtime_dependency 'json'
end
