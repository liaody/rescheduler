
require 'sinatra/base'

module Rescheduler
  class Admin < Sinatra::Base
    get '/' do
      "Hello!"
    end


    run! if app_file == $0
  end
end
