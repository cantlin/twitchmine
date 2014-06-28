require 'sinatra'
require 'json'
require 'uri'
require 'curb'
require 'filesize'
require 'trollop'

CONF = Trollop::options do
	opt :output_directory, "Output directory", :default => "#{Dir.home}/", :type => :string, :short => "-o"
	opt :viewer_threshold, "Viewer threshold", :default => 50000, :type => :integer, :short => "-t"
end

Thread.abort_on_exception = true
poll_frequency = 30 # seconds
viewer_threshold = CONF[:viewer_threshold]

class Logger
	attr_accessor :prefix

	def initialize prefix
		self.prefix = prefix
	end

	def log message
		self.class.log message, self.prefix
	end

	def self.log message, prefix = nil
		line = [Time.now, prefix].compact.map {|s| "[#{s}]"}
		puts (line << message).join ' '
	end
end

class Poller
	attr_accessor :label
	attr_accessor :sleep_time
	attr_accessor :logger

	def initialize label, sleep_time
		self.label = label
		self.sleep_time = sleep_time
		self.logger = Logger.new label

		Thread.new do
			while true
				logger.log "Polling..."

				yield(logger)

				sleep sleep_time
			end
		end
	end
end

class Twitch
	def self.get_streams(game = nil)
		req = Curl.get "https://api.twitch.tv/kraken/streams?game=#{URI.encode(game)}"
		JSON.parse(req.body_str)['streams']
	end
end

class Dota2
	def self.get uri
		req = Curl.get "#{uri}?key=F59B34C3A45FA4A0161168C9A23134F6&language=en_gb"
		JSON.parse(req.body_str)
	end

	def self.get_league_live_games
		self.get("http://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/")['result']['games']
	end

	def self.get_leagues
		data = self.get("http://api.steampowered.com/IDOTA2Match_570/GetLeagueListing/v1/")['result']['leagues']

		leagues = {}
		data.each do |l|
			leagues[l['leagueid'].to_i] = l
		end
		leagues
	end
end

latest_leagues = {}
league_poll = Poller.new("leagues", 30) do |logger|
	begin
		latest_leagues = Dota2.get_leagues
		logger.log "Found #{latest_leagues.keys.length} leagues"
	rescue StandardError => e
		logger.log "Failed to retreive leagues (#{e.to_s})"
	end
end

latest_live_games = []
live_games_poll = Poller.new("live_games", 30) do |logger|
	begin
		latest_live_games = Dota2.get_league_live_games
		logger.log("Found #{latest_live_games.length} games")
	rescue StandardError => e
		logger.log "Failed to retreive games (#{e.to_s})"
	end
end

latest_streams = []
recording_streams = []
last_refresh = DateTime.now

streams_poll = Poller.new("streams", poll_frequency) do |logger|
	logger.log("Viewer threshold is #{viewer_threshold}")
	begin
		latest_streams = Twitch.get_streams('Dota 2')
	rescue StandardError => e
		logger.log "Failed to retreive streams (#{e.to_s})"
	end
	
	eligible_streams = latest_streams.select {|s| s['viewers'].to_i >= viewer_threshold}
	logger.log("Found #{latest_streams.length} streams (highest viewers #{latest_streams[0]['viewers']}) (#{eligible_streams.length} eligible)")

	eligible_ids  = eligible_streams.map {|s| s['_id']}
	recording_ids = recording_streams.map {|s| s['_id']}
	streams_to_stop  = recording_streams.reject {|s| eligible_ids.include? s['_id']}
	streams_to_start = eligible_streams.reject {|s| recording_ids.include? s['_id']}

	logger.log("Stopping #{streams_to_stop.length} streams, starting #{streams_to_start.length} (#{recording_streams.length} currently recording)")

	streams_to_stop.each_with_index do |s, i|
		logger.log("#{s['channel']['name']} fell out of favour :(")
		Process.kill(9, s['__pid'])
		Process.wait s['__pid']
		logger.log("Killed PID #{s['__pid']}")
		recording_streams[i] = nil
	end
	recording_streams.compact!

	streams_to_start.each do |s|
		filename = "#{CONF[:output_directory]}#{s['channel']['name']}_#{Time.now.strftime '%d_%m_%Y_%H_%M_%S'}.mp4"
		s['__pid'] = fork do
			command = "livestreamer twitch.tv/#{s['channel']['name']} best -o #{filename} -f -Q"
			logger.log("[#{Process.pid}] Executing: '#{command}'")
			exec command
		end
		logger.log("Forked process #{s['__pid']} to record #{s['channel']['url']}")

		s['__started_at'] = DateTime.now
		s['__filename'] = filename
		s['__size'] = 0
		recording_streams << s
	end

	last_refresh = DateTime.now
end

file_size_poller = Poller.new("file_sizes", 10) do |logger|
	recording_streams.each do |s|
		s['__size'] = File.size(s['__filename'])
		sleep 1
	end
end

get '/' do
	o  = "<meta http-equiv=\"refresh\" content=\"10\"><title>twitchmine</title>"
	o += "<h1>twitchmine</h1>"
	o += "<p>Viewer threshold: <b>#{viewer_threshold}</b> (<a target=\"_blank\" href=\"/set_threshold/#{viewer_threshold - 1000}\">-</a> | <a target=\"_blank\" href=\"/set_threshold/#{viewer_threshold + 1000}\">+</a>)<br />"
	o += "Poll interval: <b>#{poll_frequency}</b></br>"
	o += "Last refresh: <b>#{last_refresh.to_s}</b></p>"
	o += "<h2>Currently recording</h2>"
	o += "<table>"
	o += "<tr><th>url</th><th>started at</th><th>filename</th><th>size</th><th></th></tr>"
	o += recording_streams.map {|s| "<tr><td><a href=\"#{s['channel']['url']}\">#{s['channel']['url']}</a></td><td>#{s['__started_at']}</td><td>#{s['__filename']}</td><td>#{Filesize.from(s['__size'].to_s + " B").pretty}</td><td><a href=\"/recorder/kill/#{s['_id']}\">stop</td></tr>" }.join
	o += "</table>"
	o += "<h2>Latest streams</h2>"
	o += "<table>"
	o += "<tr><th>url</th><th>viewers</th><th>status</th></tr>"
	o += latest_streams[0..10].map {|s| "<tr><td><a href=\"#{s['channel']['url']}\">#{s['channel']['url']}</a></td><td>#{s['viewers']}</td><td>#{s['channel']['status']}</td></tr>" }.join
	o += "</table>"
	o += "</table>"
	o += "<h2>Latest live games</h2>"
	o += "<table>"
	o += "<tr><th>league</th><th>viewers</th><th>dire team</th><th>radiant team</th><th>url</th></tr>"
	latest_live_games.sort_by {|g| g['spectators'].to_i * -1 }.each do |g|
		o += "<tr>"
		o += "<td>#{latest_leagues[g['league_id'].to_i]['name'] rescue nil}</td>"
		o += "<td>#{g['spectators']}</td>"
		o += "<td>#{g['dire_team']['team_name']}</td>"
		o += "<td>#{g['radiant_team']['team_name']}</td>"
		o += "<td><a href=\"#{latest_leagues[g['league_id'].to_i]['tournament_url'] rescue nil}\">#{latest_leagues[g['league_id'].to_i]['tournament_url'] rescue nil}</a></td>"
		o += "</tr>"
	end
	o += "</table>"
	o
end

get '/set_threshold/:input' do
	viewer_threshold = params[:input].to_i
	logger.log("Viewer threshold was manually set to #{viewer_threshold}")
end

get '/recorder/kill/:id' do
	id = params[:id].to_i
	recording_ids = recording_streams.map {|s| s['_id']}
	index = recording_ids.find_index(id)
	stream = recording_streams[index]

	halt 404 if !stream

	Process.kill(9, stream['__pid'])
	Process.wait stream['__pid']

	stream.delete(index)
	"killed recorder for stream #{stream['__pid']}"
end