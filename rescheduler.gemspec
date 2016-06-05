
Gem::Specification.new do |s|
  s.name        = 'rescheduler'
  s.version     = '0.5.1'
  s.date        = '2016-06-04'
  s.summary     = "A job queue for immediate and delayed jobs using Redis"
  s.description = "Rescheduler is a library that uses Redis to maintain a task queue of immediate and delayed jobs without polling."
  s.authors     = ["Dongyi Liao"]
  s.email       = 'liaody@gmail.com'
  s.files       = ["lib/rescheduler.rb", "lib/rescheduler/worker.rb", "lib/rescheduler/sync.rb"]
  s.executables = ['rescheduler_launch']
  s.homepage    = 'https://github.com/liaody/rescheduler'
  s.license     = 'BSD'

  s.add_runtime_dependency 'redis'
  s.add_runtime_dependency 'multi_json'
  #s.add_runtime_dependency 'sinatra' # For rescheduler/server only
end
