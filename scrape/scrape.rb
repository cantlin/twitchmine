require 'curb'
require 'json'
require 'uri'
require 'mysql2'
require 'sinatra'
require 'yaml'

CONF = YAML.load_file 'conf.yml'
Thread.abort_on_exception = true

class Twitch
	def self.get_streams(game = nil)
		req = Curl.get "https://api.twitch.tv/kraken/streams?game=#{URI.encode(game)}&limit=30"
		JSON.parse(req.body_str)['streams']
	end

	def self.get_games
		req = Curl.get "https://api.twitch.tv/kraken/games/top?limit=30"
		JSON.parse(req.body_str)['top']
	end
end

class PersistentStore
	@client = nil

	def self.client
		return @client if @client

		@client = Mysql2::Client.new CONF['database']
	end

	def self.query str
		self.client.query str
	end

	def self.games
		self.client.query("SELECT * FROM games")
	end

	def self.streams game_id = nil
		if game_id
			self.client.query("SELECT * FROM streams WHERE game_id = #{game_id}")
		else
			self.client.query("SELECT * FROM streams")
		end
	end

	def self.object_viewer_history object
		self.client.query(
			"SELECT DATE(recorded_at) as date, HOUR(recorded_at) as hour, #{object}_id, ROUND(AVG(viewers)) as average_viewers 
			FROM #{object}_viewer_history
			GROUP BY #{object}_id, DATE(recorded_at), HOUR(recorded_at)"
		)
	end

	def self.object_by_viewers object
		self.client.query(
			"SELECT *, ROUND(AVG(vh.viewers)) as average_viewers FROM #{object}s
			JOIN #{object}_viewer_history AS vh ON #{object}s.#{object}_id = vh.#{object}_id
			GROUP BY #{object}s.#{object}_id ORDER BY average_viewers DESC"
		)
	end

	def self.game_viewer_history
		self.object_viewer_history "game"
	end

	def self.stream_viewer_history game_id = nil
		if game_id
			self.client.query(
				"SELECT DATE(recorded_at) as date, HOUR(recorded_at) as hour, svh.stream_id as stream_id, ROUND(AVG(viewers)) as average_viewers
				FROM stream_viewer_history svh
				JOIN streams ON svh.stream_id = streams.stream_id
				WHERE streams.game_id = #{game_id}
				GROUP BY svh.stream_id, DATE(recorded_at), HOUR(recorded_at)"
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
			self.client.query(
				"SELECT *, ROUND(AVG(svh.viewers)) as average_viewers FROM streams
				JOIN stream_viewer_history AS svh ON streams.stream_id = svh.stream_id
				WHERE streams.game_id = #{game_id}
				GROUP BY streams.stream_id ORDER BY average_viewers DESC"
			)
		else
			self.object_by_viwers "stream"
		end
	end

	def self.init
		sql = ["CREATE TABLE IF NOT EXISTS games (
		   game_id int(11) NOT NULL,
		   name varchar(255) NOT NULL,
		  PRIMARY KEY (game_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"CREATE TABLE IF NOT EXISTS streams (
		   stream_id int(11) NOT NULL,
		   name varchar(255) NOT NULL,
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

		sql.each {|q| self.client.query(q) }
	end
end

PersistentStore.init
puts "scrapin' :)"

Thread.new do
	while true do
		mysql_client = Mysql2::Client.new CONF['database']
		current_games = Twitch.get_games

		# Track any game in the top five, if we aren't already
		Twitch.get_games[0..4].each do |game|
			mysql_client.query(
				"INSERT IGNORE INTO games (game_id, name) 
				 VALUES (#{game['game']['_id']}, '#{game['game']['name']}')")
		end

		games_to_track = mysql_client.query("SELECT * FROM games")

		# For all games we're tracking, try and find the number
		# of viewers from the Twitch response and record it
		games_to_track.each do |game|
			current_data = current_games.find {|g| g['game']['_id'] == game['game_id']}
			next if current_data.nil? # game dropped out of most viewed

			puts "[#{Time.new.to_s}] game '#{game['name']}' has #{current_data['viewers']} viewers #{[";)", ":)", ":D"][rand(3)]}"

			mysql_client.query(
				"INSERT INTO game_viewer_history (game_id, viewers)
				 VALUES (#{game['game_id']}, #{current_data['viewers']})")
		end

		sleep 900 # 15 minutes
	end
end

Thread.new do
	while true
		mysql_client = Mysql2::Client.new CONF['database']
		games = mysql_client.query("SELECT * FROM games")

		games.each do |game|
			current_streams = Twitch.get_streams(game['name'])

			# Track any stream in the top five, if we aren't already
			current_streams[0..4].each do |stream|
				mysql_client.query(
					"INSERT IGNORE INTO streams (stream_id, name, game_id) 
					 VALUES (#{stream['channel']['_id']}, '#{stream['channel']['name']}', #{game['game_id']})")
			end

			# Record viewers for all game streams
			streams = mysql_client.query("SELECT * FROM streams WHERE game_id = #{game['game_id']}")
			streams.each do |stream|
				current_data = current_streams.find {|s| s['channel']['_id'] == stream['stream_id']}
				next if current_data.nil? # stream dropped out of most viewed

				puts "[#{Time.new.to_s}] stream '#{stream['name']}' (playing #{game['name']}) has #{current_data['viewers']} viewers #{[";)", ":)", ":D"][rand(3)]}"

				status = mysql_client.escape(current_data['channel']['status'])
				mysql_client.query(
					"INSERT INTO stream_viewer_history (stream_id, current_status, viewers)
					 VALUES (#{stream['stream_id']}, '#{status}', #{current_data['viewers']})")
			end

			sleep 10
		end

		sleep 720 # 12 minutes
	end
end

def results_to_spreadsheet_array opts
	raise ArgumentError unless opts.keys == [:cols, :rows, :key, :prop]

	ranges = opts[:rows].group_by {|r| "#{r['date']} #{r['hour']}:00-#{r['hour']}:59"}
	puts ranges.to_json

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
		o['streams_url'] = "http://#{request.host}:#{request.port}/streams/#{o['game_id']}"
		o['streams_spreadsheet_url'] = "http://#{request.host}:#{request.port}/streams/#{o['game_id']}/spreadsheet"
		o
	end.to_json
end

get '/streams/?' do
	results = PersistentStore.streams
	results.to_a.to_json
end

get '/streams/:game_id/?' do
	results = PersistentStore.streams(params[:game_id])
	results.to_a.to_json
end

get '/games/spreadsheet/?' do
	results_to_spreadsheet_array(
		cols: PersistentStore.games_by_viewers,
		rows: PersistentStore.game_viewer_history,
		key: "game_id",
		prop: "average_viewers"
	).to_json
end

get '/streams/spreadsheet' do
	results_to_spreadsheet_array(
		cols: PersistentStore.streams_by_viewers,
		rows: PersistentStore.stream_viewer_history,
		key: "stream_id",
		prop: "average_viewers"
	).to_json
end

get '/streams/:game_id/spreadsheet/?' do
	results_to_spreadsheet_array(
		cols: PersistentStore.streams_by_viewers(params[:game_id]),
		rows: PersistentStore.stream_viewer_history(params[:game_id]),
		key: "stream_id",
		prop: "average_viewers"
	).to_json
end