require 'rails_support/all'

class ApplicationController < ActionController::Base
  protect_from_forgery

  def test_query
    render json: JoowingData.process_query('load_by_key', [{'app'=>'task_app', 'model'=>'TaskCheck'}])
  end

  def test_faraday
    render json: JoowingData.process_query2('load_by_key', [{'app'=>'task_app', 'model'=>'TaskCheck'}])
  end
end
