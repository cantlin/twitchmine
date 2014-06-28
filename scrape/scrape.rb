require 'curb'
require 'json'
require 'uri'
require 'mysql2'
require 'sinatra'
require 'yaml'

CONF = YAML.load_file 'conf.yml'
Thread.abort_on_exception = true

class Twitch
	def self.get_body url
		req = Curl.get url
		throw StandardError.new("Twitch API unhappy! (returned #{req.response_code})") unless /2\d\d/.match(req.response_code.to_s)
		req.body_str
	end

	def self.get_streams(game = nil)
		body = self.get_body "https://api.twitch.tv/kraken/streams?app_contact=cantlin@ashrowan.com&game=#{URI.encode(game)}&limit=30"
		JSON.parse(body)['streams']
	end

	def self.get_games
		body = self.get_body "https://api.twitch.tv/kraken/games/top?app_contact=cantlin@ashrowan.com&limit=30"
		JSON.parse(body)['top']
	end
end

class PersistentStore
	def self.with_connection
		begin
			client = Mysql2::Client.new CONF['database']

			yield(client)

			client.close if client
		rescue StandardError => e
			puts "Could not connect to database", e.message
		end
	end

	def self.query q
		self.with_connection |client|
			begin
				client.query q
			rescue StandardError => e
				puts "Query failed", q, e.message, e.backtrace
			ensure
				client.close
			end
		end
	end

	def self.batch queries, throw_on_error = false
		self.with_connection |client|
			results = queries.map do |q|
				begin
					client.query q
				rescue StandardError => e
					puts "Query failed", str, e.message, e.backtrace

					if throw_on_error
						client.close
						throw e
					else
						false
					end
				end
			end

			client.close
			results
		end
	end

	def self.games
		self.query("SELECT * FROM games")
	end

	def self.streams game_id = nil
		if game_id
			self.query("SELECT * FROM streams WHERE game_id = #{game_id}")
		else
			self.query("SELECT * FROM streams")
		end
	end

	def self.object_viewer_history object
		self.query(
			"SELECT DATE(recorded_at) as date, HOUR(recorded_at) as hour, #{object}_id, ROUND(AVG(viewers)) as average_viewers 
			FROM #{object}_viewer_history
			GROUP BY #{object}_id, DATE(recorded_at), HOUR(recorded_at)
			ORDER BY recorded_at"
		)
	end

	def self.object_by_viewers object
		self.query(
			"SELECT *, SUM(vh.viewers) as viewers FROM #{object}s
			JOIN #{object}_viewer_history AS vh ON #{object}s.#{object}_id = vh.#{object}_id
			GROUP BY #{object}s.#{object}_id ORDER BY viewers DESC"
		)
	end

	def self.game_viewer_history
		self.object_viewer_history "game"
	end

	def self.stream_viewer_history game_id = nil
		if game_id
			self.query(
				"SELECT DATE(recorded_at) as date, HOUR(recorded_at) as hour, svh.stream_id as stream_id, ROUND(AVG(viewers)) as average_viewers
				FROM stream_viewer_history svh
				JOIN streams ON svh.stream_id = streams.stream_id
				WHERE streams.game_id = #{game_id}
				GROUP BY svh.stream_id, DATE(recorded_at), HOUR(recorded_at)
				ORDER BY recorded_at"
			)
		else
			self.object_viewer_history "stream"
		end
	end

	def self.games_by_viewers
		self.object_by_viewers "game"
	end

	def self.streams_by_viewers game_id = nil
		if game_id
			self.query(
				"SELECT *, SUM(svh.viewers) as viewers FROM streams
				JOIN stream_viewer_history AS svh ON streams.stream_id = svh.stream_id
				WHERE streams.game_id = #{game_id}
				GROUP BY streams.stream_id ORDER BY viewers DESC"
			)
		else
			self.object_by_viwers "stream"
		end
	end

	def self.init
		sql = ["CREATE TABLE IF NOT EXISTS games (
		   game_id int(11) NOT NULL,
		   name varchar(255) NOT NULL,
		   record_streams int(1) NOT NULL DEFAULT 0,
		  PRIMARY KEY (game_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"CREATE TABLE IF NOT EXISTS streams (
		   stream_id int(11) NOT NULL,
		   name varchar(255) NOT NULL,
		   display_name varchar(255) NOT NULL,
		   game_id int(11) NOT NULL,
		  PRIMARY KEY (stream_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"CREATE TABLE IF NOT EXISTS game_viewer_history (
		   history_id int(11) NOT NULL AUTO_INCREMENT,
		   recorded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   game_id int(11) NOT NULL,
		   viewers int(11) NOT NULL,
		  PRIMARY KEY (history_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"CREATE TABLE IF NOT EXISTS stream_viewer_history (
		   history_id int(11) NOT NULL AUTO_INCREMENT,
		   recorded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   current_status varchar(255) NOT NULL,
		   stream_id int(11) NOT NULL,
		   viewers int(11) NOT NULL,
		  PRIMARY KEY (history_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8"]

		sql.each {|q| self.query q }
	end
end

PersistentStore.init
puts "scrapin' :)"

Thread.new do
	while true do
		begin
			current_games = Twitch.get_games
		rescue StandardError => e
			puts "[#{Time.new.to_s}] Error retrieving viewers for top games", e.message, e.backtrace
			sleep 300
			next
		end

		start_time = Time.now

		# Track any game in the top five, if we aren't already
		PersistentStore.batch current_games[0..4].map do |game|
			name = mysql_client.escape game['game']['name']
			"INSERT IGNORE INTO games (game_id, name) 
			 VALUES (#{game['game']['_id']}, '#{name}')"
		end

		games_to_track = PersistentStore.query("SELECT * FROM games")

		# For all games we're tracking, try and find the number
		# of viewers from the Twitch response and record it
		insert_queries = []
		games_to_track.each do |game|
			current_data = current_games.find {|g| g['game']['_id'] == game['game_id']}
			next if current_data.nil? # game dropped out of most viewed

			insert_queries.push("INSERT INTO game_viewer_history (game_id, viewers)
			VALUES (#{game['game_id']}, #{current_data['viewers']})")
		end
		PersistentStore.batch insert_queries

		puts "[#{Time.new.to_s}] Saved viewers for #{games_to_track.to_a.length} games in #{(Time.now - start_time).to_i}s (1 request)"
		sleep 900 # ten minutes
	end
end

Thread.new do
	while true
		games = PersistentStore.query("SELECT * FROM games WHERE record_streams = 1")
		start_time = Time.now

		games.each do |game|
			begin
				current_streams = Twitch.get_streams(game['name'])
			rescue StandardError => e
				puts "[#{Time.new.to_s}] Error retrieving streams for #{game['name']}", e.message, e.backtrace
				sleep 300
				next
			end

			# Track any stream in the top five, if we aren't already
			PersistentStore.batch current_streams[0..4].map do |stream|
				name = mysql_client.escape stream['channel']['name']
				"INSERT IGNORE INTO streams (stream_id, name, display_name, game_id) 
				 VALUES (#{stream['channel']['_id']}, '#{name}', '#{stream['channel']['display_name']}', #{game['game_id']})")
			end

			# Record viewers for all game streams
			streams = PersistentStore.query("SELECT * FROM streams WHERE game_id = #{game['game_id']}")
			insert_queries = []
			streams.each do |stream|
				current_data = current_streams.find {|s| s['channel']['_id'] == stream['stream_id']}
				next if current_data.nil? # stream dropped out of most viewed

				status = mysql_client.escape(current_data['channel']['status'])
				insert_queries.push("INSERT INTO stream_viewer_history (stream_id, current_status, viewers) 
				VALUES (#{stream['stream_id']}, '#{status}', #{current_data['viewers']})")
			end
			PersistentStore.batch insert_queries
			sleep 3
		end

		puts "[#{Time.new.to_s}] Saved viewers for all streams in #{(Time.now - start_time).to_i}s (#{games.to_a.length} requests)"
		sleep 900
	end
end

def results_to_spreadsheet_array opts
	raise ArgumentError unless opts.keys == [:cols, :rows, :key, :prop]

	ranges = opts[:rows].group_by {|r| "#{r['date']} #{r['hour'] + 1}:00"}

	output = [["Date Range"]]
	opts[:cols].each do |column|
		output[0] << column["name"]

		ranges.each do |range, object|
			row = output.find {|r| r[0] == range} || [range]
			row << (object.find {|d| d[opts[:key]] == column[opts[:key]]} || {})[opts[:prop]]
			output << row if row.length == 2
		end
	end

	output
end

before { content_type :json }

get '/games/?' do
	PersistentStore.games.map do |o|
		if o['record_streams'] == 1
			o['streams_url'] = "http://#{request.host}:#{request.port}/game/#{o['game_id']}/streams"
			o['streams_spreadsheet_url'] = "http://#{request.host}:#{request.port}/game/#{o['game_id']}/streams/spreadsheet"
		end
		o
	end.to_json
end

get '/games/spreadsheet/?' do
	results_to_spreadsheet_array(
		cols: PersistentStore.games_by_viewers,
		rows: PersistentStore.game_viewer_history,
		key: "game_id",
		prop: "average_viewers"
	).to_json
end

get '/streams/?' do
	results = PersistentStore.streams
	results.to_a.to_json
end

get '/streams/spreadsheet/?' do
	results_to_spreadsheet_array(
		cols: PersistentStore.streams_by_viewers,
		rows: PersistentStore.stream_viewer_history,
		key: "stream_id",
		prop: "average_viewers"
	).to_json
end

get '/game/:game_id/streams/?' do
	results = PersistentStore.streams(params[:game_id])
	results.to_a.to_json
end

get '/game/:game_id/streams/spreadsheet/?' do
	results_to_spreadsheet_array(
		cols: PersistentStore.streams_by_viewers(params[:game_id]),
		rows: PersistentStore.stream_viewer_history(params[:game_id]),
		key: "stream_id",
		prop: "average_viewers"
	).to_json
end