require 'rescheduler'
require 'active_job'

module ActiveJob
  module QueueAdapters
    # == Rescheduler adapter for Active Job
    #   Rails.application.config.active_job.queue_adapter = :rescheduler
  class ReschedulerAdapter
      def enqueue(job) #:nodoc:
        Rescheduler.enqueue(queue: job.queue_name, class: JobWrapper.name, job_data: job.serialize)
      end

      def enqueue_at(job, timestamp) #:nodoc:
        Rescheduler.enqueue(queue: job.queue_name, class: JobWrapper.name, job_data: job.serialize, due_at: timestamp)
      end

      class JobWrapper #:nodoc:
        class << self
          def perform(job_data)
            Base.execute job_data
          end
        end
      end
    end
  end
end
