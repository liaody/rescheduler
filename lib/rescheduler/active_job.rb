
module Rescheduler

  module ActiveJob
    extend ActiveSupport::Concern

    module ClassMethods

      def add_active_jobs(*queue_names)
        queues = queue_names.map(&:to_s)

        queues.each do |que|
          Rescheduler.job(que) do |data|
            if [:class, :job_data].all? { |k| data[k].present? }
              cls = ActiveSupport::Inflector.constantize(data[:class])
              job_data = data[:job_data]
              # stringify_keys to adapt to active_job's lib implementation
              cls.perform(job_data.stringify_keys) if cls.respond_to?(:perform)
            end
          end
        end

      end

    end
  end

end