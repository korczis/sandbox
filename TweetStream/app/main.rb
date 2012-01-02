#! /usr/bin/env ruby

require 'rubygems'
require 'tweetstream'
require 'yaml'
require 'Bsl'

class TweetStreamApp < Bsl::Application
	@@DEFAULT_DB_TYPE = "sql"
	
	@@DEFAULT_SQL_HOST = "localhost"
	@@DEFAULT_SQL_PORT = 5432
	@@DEFAULT_SQL_DB = "tweetstream"
	@@DEFAULT_SQL_USER = "host"
	@@DEFAULT_SQL_PASSWORD = "password"
	
	@@DEFAULT_NOSQL_HOST = "localhost"
	@@DEFAULT_NOSQL_PORT = 5984
	@@DEFAULT_NOSQL_DB = "tweetstream"
	@@DEFAULT_NOSQL_USER = "host"
	@@DEFAULT_NOSQL_PASSWORD = "password"
	
	@@DEFAULT_CONSUMER_KEY = "twitter_consumer_key"
	@@DEFAULT_CONSUMER_SECRET = "twitter_consumer_secret"
	@@DEFAULT_OAUTH_TOKEN = "twitter_ouath_token"
	@@DEFAULT_OAUTH_TOKEN_SECRET = "twitter_token_secret"
	
	def initialize(opts = {})
		super(opts)
		
		options['db_type'] = @@DEFAULT_DB_TYPE
		
		options['postgresql_host'] = @@DEFAULT_SQL_HOST
		options['postgresql_port'] = @@DEFAULT_SQL_PORT
		options['postgresql_db'] = @@DEFAULT_SQL_DB
		options['postgresql_user'] = @@DEFAULT_SQL_USER
		options['postgresql_password'] = @@DEFAULT_SQL_PASSWORD
		
		options['couchdb_host'] = @@DEFAULT_NOSQL_HOST
		options['couchdb_port'] = @@DEFAULT_NOSQL_PORT
		options['couchdb_db'] = @@DEFAULT_NOSQL_DB
		options['couchdb_user'] = @@DEFAULT_NOSQL_USER
		options['couchdb_password'] = @@DEFAULT_NOSQL_PASSWORD
		
		options['consumer_key'] = @@DEFAULT_CONSUMER_KEY
		options['consumer_secret'] = @@DEFAULT_CONSUMER_SECRET
		options['oauth_token'] = @@DEFAULT_OAUTH_TOKEN
		options['oauth_token_secret'] = @@DEFAULT_OAUTH_TOKEN_SECRET
	end
	
	def initialize_options(opts = {})
		super(opts)
		
		# DB Type
		opts.on('--db-type STRING', "Type of DB [postgresql, couchdb]") do |val|
			options['db_type'] = val
		end
		
		
		# SQL
		opts.on('--postgresql-host STRING', "PostgreSQL Hostname") do |val|
			options['postgresql_host'] = val
		end
		
		opts.on('--postgresql-port STRING', "PostgreSQL Port") do |val|
			options['postgresql_port'] = val
		end
		
		opts.on('--postgresql-db STRING', "PostgreSQL Database Name") do |val|
			options['postgresql_db'] = val
		end
		
		opts.on('--postgresql-user STRING', "PostgreSQL Username") do |val|
			options['postgresql_user'] = val
		end
		
		opts.on('--postgresql-password STRING', "PostgreSQL Password") do |val|
			options['postgresql_password'] = val
		end

		# CouchDB
		opts.on('--couchdb-host STRING', "CouchDB Hostname") do |val|
			options['couchdb_host'] = val
		end
		
		opts.on('--couchdb-port STRING', "CouchDB Port") do |val|
			options['couchdb_port'] = val
		end
		
		opts.on('--couchdbl-db STRING', "CouchDB Database Name") do |val|
			options['couchdb_db'] = val
		end
		
		opts.on('--couchdb-user STRING', "CouchDB Username") do |val|
			options['couchdb_user'] = val
		end
		
		opts.on('--couchdb-password STRING', "CouchDB Password") do |val|
			options['couchdb_password'] = val
		end
		
		# Twitter
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
		
		conn = PGconn.connect(options['postgresql_host'], options['postgresql_port'], '', '', options['postgresql_db'], options['postgresql_user'], options['postgresql_password'])
		
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
