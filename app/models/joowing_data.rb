#encoding: utf-8
require 'zlib'
require 'base64'

class JoowingData < ActiveResource::Base
  include JoowingObj::Port::ActiveResource

  cattr_accessor :query_identifier_processor
  self.query_identifier_processor = proc do
    '6@1'
  end

  self.primary_key  = 'uuid'
  self.element_name = 'data'
  #self.site = 'http://127.0.0.1:3000/cache_processor'
  #self.site = 'http://127.0.0.1:3000/api/v1'
  self.site         = 'http://192.168.10.249:30021/api/v1'

  class CreatableHash < Hash
    def initialize(another_hash)
      super()
      update(another_hash)
    end
  end

  Key    = CreatableHash
  Value  = CreatableHash
  Target = CreatableHash


  def from_persistance_obj_to_data
    h = {
        key:            self.key,
        value:          self.value,
        synced:         true,
        last_synced_at: self.last_synced_at,
        #error: self.error,
        #error_trace: self.error_trace,
        creator:        self.creator,
        targets:        self.targets,
        uuid:           self.uuid,
        #key_index_text: self.key_index_text,
        as:             self.as,
        created_at:     self.created_at,
        updated_at:     self.updated_at,
        #sha1: self.sha1
    }

    %w{ error error_trace key_index_text sha1 }.each do |m|
      if self.respond_to? m.to_sym
        h.update(m.to_sym => self.send(m))
      end
    end

    h
  end

  def clear
    self.destroy
  end

  class << self
    def ar_load(uuid)
      find(uuid)
    rescue ActiveResource::ResourceNotFound => e
      nil
    end

    %w{ load_by_as load_by_key_type load_by_key load_by_keys }.each do |query_method_name|
      define_method query_method_name do |*args|
        process_query(query_method_name, args)
      end
    end

    def query(opt = {})
      if opt.has_key?('key')
        self.load_by_key(opt['key'])
      elsif opt.has_key?('keys')
        self.load_by_keys(opt['keys'] || [])
      else
        []
      end
    end

    def process_query(command, args = [])
      start = Time.now
      if query_identifier.nil?
        log_error '所有的查询需要查询上下文的介入'
        return []
      end

      query_url    = "#{self.prefix}query.#{format.extension}"
      query_params = { command: command, data_lake_stub: query_identifier, raw: true }

      # encode command, for query
      args_txt     = JSON.dump({ args: args }).try do |encoded_text|
        StringIO.new.tap do |sio|
          Zlib::GzipWriter.new(sio).tap do |z|
            z.write(encoded_text)
            z.close
          end
        end.string
      end.try { |zipped_text| Base64.encode64(zipped_text) }

      query_params[:raw_args] = args_txt
      query_url               = "#{query_url}?#{query_params.to_query}"

      log_info query_url
      raw_datum = format.decode(connection.get(query_url, headers).body)
      instantiate_collection(raw_datum, {}).tap do
        log_info "查询结束, 消耗时间: #{((Time.now.to_f - start.to_f) * 1000).to_i}ms"
      end
    rescue ActiveResource::ClientError => e
      log_error "query failed due to client error: #{e.to_s}"
      log_error "message: #{e.message}"
      log_error "command: #{command}"
      log_error "args: #{args.inspect}"
      []
    end


    %w{ unsynceds commit_sync all create_by }.each do |nonreponse_method_name|
      define_method nonreponse_method_name do |*args|
        []
      end
    end

    def process_query2(command, args = [])
      start = Time.now
      if query_identifier.nil?
        log_error '所有的查询需要查询上下文的介入'
        return []
      end

      query_url    = "#{self.prefix}query.#{format.extension}"
      query_params = { command: command, data_lake_stub: query_identifier, raw: true }

      # encode command, for query
      args_txt     = JSON.dump({ args: args }).try do |encoded_text|
        StringIO.new.tap do |sio|
          Zlib::GzipWriter.new(sio).tap do |z|
            z.write(encoded_text)
            z.close
          end
        end.string
      end.try { |zipped_text| Base64.encode64(zipped_text) }

      query_params[:raw_args] = args_txt
      query_url               = "#{query_url}?#{query_params.to_query}"

      log_info query_url
      raw_datum = format.decode(farady.get(query_url).body)
      instantiate_collection(raw_datum, {}).tap do
        log_info "查询结束, 消耗时间: #{((Time.now.to_f - start.to_f) * 1000).to_i}ms"
      end
    rescue ActiveResource::ClientError => e
      log_error "query failed due to client error: #{e.to_s}"
      log_error "message: #{e.message}"
      log_error "command: #{command}"
      log_error "args: #{args.inspect}"
      []
    end

    def farady
      @farady ||= begin
        Faraday.new(:url => 'http://192.168.10.249:30021') do |faraday|
          faraday.request :url_encoded # form-encode POST params
          faraday.adapter :em_synchrony # make requests with Net::HTTP
        end
      end
    end


      def query_identifier
        self.query_identifier_processor.call
      end
    end
  end