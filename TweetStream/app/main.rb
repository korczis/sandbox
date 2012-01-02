#! /usr/bin/env ruby

require 'rubygems'
require 'tweetstream'
require 'yaml'
require 'Bsl'

class TweetStreamApp < Bsl::Application
	@@DEFAULT_SQL_HOST = "localhost"
	@@DEFAULT_SQL_PORT = 5432
	@@DEFAULT_SQL_DB = "tweetstream"
	@@DEFAULT_SQL_USER = "host"
	@@DEFAULT_SQL_PASSWORD = "password"
	
	@@DEFAULT_CONSUMER_KEY = "twitter_consumer_key"
	@@DEFAULT_CONSUMER_SECRET = "twitter_consumer_secret"
	@@DEFAULT_OAUTH_TOKEN = "twitter_ouath_token"
	@@DEFAULT_OAUTH_TOKEN_SECRET = "twitter_token_secret"
	
	def initialize(opts = {})
		super(opts)
			
		options['consumer_key'] = @@DEFAULT_CONSUMER_KEY
		options['consumer_secret'] = @@DEFAULT_CONSUMER_SECRET
		options['oauth_token'] = @@DEFAULT_OAUTH_TOKEN
		options['oauth_token_secret'] = @@DEFAULT_OAUTH_TOKEN_SECRET
		
		options['sql_host'] = @@DEFAULT_SQL_HOST
		options['sql_port'] = @@DEFAULT_SQL_PORT
		options['sql_db'] = @@DEFAULT_SQL_DB
		options['sql_user'] = @@DEFAULT_SQL_USER
		options['sql_password'] = @@DEFAULT_SQL_PASSWORD
	end
	
	def initialize_options(opts = {})
		super(opts)
		
		opts.on('--sql-host STRING', "PostgreSQL Hostname") do |val|
			options['sql_host'] = val
		end
		
		opts.on('--sql-port STRING', "PostgreSQL Port") do |val|
			options['sql_port'] = val
		end
		
		opts.on('--sql-db STRING', "PostgreSQL Database Name") do |val|
			options['sql_db'] = val
		end
		
		opts.on('--sql-user STRING', "PostgreSQL Username") do |val|
			options['sql_user'] = val
		end
		
		opts.on('--sql-password STRING', "PostgreSQL Password") do |val|
			options['sql_password'] = val
		end
		
		opts.on('--sql_host STRING', "PostgreSQL Hostname") do |val|
			options['sql_host'] = val
		end

		opts.on('--consumer-key STRING', "Twitter Consumer Key") do |val|
			options['consumer_key'] = val
		end

		opts.on('--consumer-secret STRING', "Twitter Consumer Secret") do |val|
			options['consumer_secret'] = val
		end

		opts.on('--auth-token STRING', "Twitter Consumer OAuth Token") do |val|
			options['oauth_token'] = val
		end		

		opts.on('--auth-secret STRING', "Twitter Consumer OAuth Secret") do |val|
			options['oauth_token_secret'] = val
		end
	end
	
	def main()
		super()

		
		conn = PGconn.connect(options['sql_host'], options['sql_port'], '', '', options['sql_db'], options['sql_user'], options['sql_password'])
		
		TweetStream.configure do |config|
			config.consumer_key = options['consumer_key']
			config.consumer_secret = options['consumer_secret']
			config.oauth_token = options['oauth_token']
			config.oauth_token_secret = options['oauth_token_secret']
			config.auth_method = :oauth
			config.parser   = :yajl
		end

		# This will pull a sample of all tweets based on
		# your Twitter account's Streaming API role.
		t = nil
		TweetStream::Client.new.sample do |status|
			t_now = Time.now
			if(t == nil || t.sec != t_now.sec)
				puts ""
				puts status.text
				puts t_now.to_s
				t = t_now
				STDOUT.flush
			end
			
			# The status object is a special Hash with
			# method access to its keys.
			# puts "#{status.text}"
			print '.'
			
			status_text = status.text.gsub("'", "")
			query = "insert into tweets (status) values('#{status_text}');"
			#puts "#{query}"
			begin
				res = conn.exec(query)
			rescue Exception => e
				puts "EXCEPTION: #{e.to_s}"
			end
		end
	end
end

if $0 == __FILE__
	TweetStreamApp.new.main()
end
