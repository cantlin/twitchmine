require 'sinatra'
require 'json'
require 'uri'
require 'curb'
require 'filesize'

Thread.abort_on_exception = true
poll_frequency = 30 # seconds
gold_threshold = 25000

class Util
	def self.log message
		puts "[#{Time.now}] #{message}"
	end
end

class Twitch
	def self.get_streams(game = nil)
		req = Curl.get "https://api.twitch.tv/kraken/streams?game=#{URI.encode(game)}"
		JSON.parse(req.body_str)['streams']
	end
end

latest_streams = []
recording_streams = []
last_refresh = DateTime.now

Thread.new do
	while true do
		Util.log("Refreshing streams (viewer threshold is #{gold_threshold})...")
		begin
			latest_streams = Twitch.get_streams('Dota 2')
		rescue StandardError => e
			Util.log("Failed to retreive streams (how's the network?)")
		end
		
		eligible_streams = latest_streams.select {|s| s['viewers'].to_i >= gold_threshold}
		Util.log("Found #{latest_streams.length} streams (highest viewers #{latest_streams[0]['viewers']}) (#{eligible_streams.length} eligible)")

		eligible_ids  = eligible_streams.map {|s| s['_id']}
		recording_ids = recording_streams.map {|s| s['_id']}
		streams_to_stop  = recording_streams.reject {|s| eligible_ids.include? s['_id']}
		streams_to_start = eligible_streams.reject {|s| recording_ids.include? s['_id']}

		Util.log("Stopping #{streams_to_stop.length} streams, starting #{streams_to_start.length} (#{recording_streams.length} currently recording)")

		streams_to_stop.each_with_index do |s, i|
			Util.log("#{s['channel']['name']} fell out of favour :(")
			Process.kill(9, s['__pid'])
			Process.wait s['__pid']
			Util.log("Killed PID #{s['__pid']}")
			recording_streams[i] = nil
		end
		recording_streams.compact!

		streams_to_start.each do |s|
			filename = "#{s['channel']['name']}.mp4"
			s['__pid'] = fork do
				command = "livestreamer twitch.tv/#{s['channel']['name']} best -o #{filename} -f -Q"
				Util.log("[#{Process.pid}] Executing: '#{command}'")
				exec command
			end
			Util.log("Forked process #{s['__pid']} to record #{s['channel']['url']}")

			s['__started_at'] = DateTime.now
			s['__filename'] = filename
			s['__size'] = 0
			recording_streams << s
		end

		Util.log("Sleeping #{poll_frequency}...")
		last_refresh = DateTime.now
		sleep poll_frequency
	end
end

Thread.new do
	while true do
		recording_streams.each do |s|
			s['__size'] = File.size(s['__filename'])
			sleep 1
		end
		sleep 10
	end
end

get '/' do
	o  = "<meta http-equiv=\"refresh\" content=\"10\"><title>twitchmine</title>"
	o += "<h1>twitchmine</h1>"
	o += "<p>Viewer threshold: <b>#{gold_threshold}</b> (<a target=\"_blank\" href=\"/set_threshold/#{gold_threshold - 1000}\">-</a> | <a target=\"_blank\" href=\"/set_threshold/#{gold_threshold + 1000}\">+</a>)<br />"
	o += "Poll interval: <b>#{poll_frequency}</b></br>"
	o += "Last refresh: <b>#{last_refresh.to_s}</b></p>"
	o += "<h2>Currently recording</h2>"
	o += "<table>"
	o += "<tr><th>url</th><th>started at</th><th>filename</th><th>size</th><th></th></tr>"
	o += recording_streams.map {|s| "<tr><td><a href=\"#{s['channel']['url']}\">#{s['channel']['url']}</a></td><td>#{s['__started_at']}</td><td>#{s['__filename']}</td><td>#{Filesize.from(s['__size'].to_s + " B").pretty}</td><td><a href=\"/recorder/kill/#{s['_id']}\">stop</td></tr>" }.join
	o += "</table>"
	o += "<h2>Latest streams</h2>"
	o += "<table>"
	o += "<tr><th>url</th><th>viewers</th></tr>"
	o += latest_streams.map {|s| "<tr><td><a href=\"#{s['channel']['url']}\">#{s['channel']['url']}</a></td><td>#{s['viewers']}</td></tr>" }.join
	o += "</table>"
	o
end

get '/set_threshold/:input' do
	gold_threshold = params[:input].to_i
	Util.log("Viewer threshold was manually set to #{gold_threshold}")
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