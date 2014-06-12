require 'curb'
require 'json'
require 'uri'
require 'mysql2'
require 'sinatra'
require 'yaml'

CONF = YAML.load_file 'conf.yml'

class Twitch
	def self.get_streams(game = nil)
		req = Curl.get "https://api.twitch.tv/kraken/streams?game=#{URI.encode(game)}"
		JSON.parse(req.body_str)['streams']
	end

	def self.get_games
		req = Curl.get "https://api.twitch.tv/kraken/games/top?limit=30"
		JSON.parse(req.body_str)['top']
	end
end

mysql_client = Mysql2::Client.new CONF['database']

mysql_client.query("CREATE TABLE IF NOT EXISTS games (
   game_id int(11) NOT NULL,
   name varchar(255) NOT NULL,
  PRIMARY KEY (game_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8")

mysql_client.query("CREATE TABLE IF NOT EXISTS game_viewer_history (
   history_id int(11) NOT NULL AUTO_INCREMENT,
   recorded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   game_id int(11) NOT NULL,
   viewers int(11) NOT NULL,
  PRIMARY KEY (history_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8")

Twitch.get_games[0..9].each do |game|
	mysql_client.query(
		"INSERT IGNORE INTO games (game_id, name) 
		 VALUES (#{game['game']['_id']}, '#{game['game']['name']}')")
end

puts "scrapin' :)"
current_games = []

Thread.new do
	while true do
		games_to_track = mysql_client.query("SELECT * FROM games")
		current_games = Twitch.get_games

		games_to_track.each do |game|
			current_data = current_games.find {|g| g['game']['_id'] == game['game_id']}
			
			puts "[#{Time.new.to_s}] #{game['name']} has #{current_data['viewers']} viewers #{[";)", ":)", ":D"][rand(3)]}"

			mysql_client.query(
			"INSERT INTO game_viewer_history (game_id, viewers)
			 VALUES (#{game['game_id']}, #{current_data['viewers']})")
		end

		sleep 300
	end
end

get '/' do
	"<a href=\"/current\">current</a><br /><a href=\"/games/csv\">games csv</a>"
end

get '/current' do
	content_type :json
	current_games.map {|g| { name: g['game']['name'], viewers: g['viewers'] } }.sort_by {|g| g[:viewers] * -1}.to_json
end

get '/games/csv' do
	# content_type :csv
	ranges = {}

	results = mysql_client.query(
		"SELECT DATE(recorded_at) as day, HOUR(recorded_at) as hour, game_id, ROUND(AVG(viewers)) as average_viewers 
		FROM game_viewer_history
		GROUP BY game_id, DATE(recorded_at), HOUR(recorded_at)")

	results.each do |row|
		range = "#{row['day']} #{row['hour']}:00-#{row['hour']}:59"

		ranges[range] ||= []
		ranges[range] << {
			'game_id' => row['game_id'],
			'viewers' => row['average_viewers']
		}
	end

	games = mysql_client.query(
		"SELECT *, AVG(gvh.viewers) as average_viewers FROM games
		JOIN game_viewer_history AS gvh ON games.game_id = gvh.game_id
		GROUP BY games.game_id ORDER BY average_viewers DESC"
	)

	rows = [["Date Range"]]
	games.each do |game|
		rows[0] << game['name']

		ranges.each do |range, data|
			row = rows.find {|r| r[0] == range} || [range]
			row << (data.find {|d| d['game_id'] == game['game_id']} || {})['viewers']
			rows << row if row.length == 2
		end
	end

	rows.map {|r| r.join(',')}.join("\n")
end