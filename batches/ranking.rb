require 'bundler'
Bundler.require

class Ranking
  attr_reader :logger
  ROOT_PATH = File.expand_path('../../', __FILE__)
  CONFIG = Hashie::Mash.new YAML.load_file File.join(ROOT_PATH, '/config/config.yml')
  CONST = Hashie::Mash.new YAML.load_file File.join(ROOT_PATH, '/config/const.yml')

  def self.execute(env)
    batch = new
    puts "=== #{batch.name} Start"
    begin
      batch.ranking_tally(env)
    rescue => e
      puts [e.class, e.message, e.backtrace].join("\n")
    end
    puts "=== #{batch.name} End"
  end

  def ranking_tally(env)
    CONST.ranking.types.each { |type|
      dbh = Sequel.mysql(CONFIG[env].database.database, :host=>CONFIG[env].database.host, :user=>CONFIG[env].database.user, :password=>CONFIG[env].database.pass, :port=>CONFIG[env].database.port)
      column = type.column
      table = type.table
      dbh.transaction do
        dbh[:"#{table}"].truncate
        dbh[:user_hobbies]
          .where(:is_active => 1)
          .select{[:"#{column}", Sequel.as(count(:id), :count)]}
          .group(:"#{column}")
          .each { |data|
            dbh[:"#{table}"].insert(:id=>data[:"#{column}"], :count=>data[:count])
          }
      end
    }
  end

  def name
    self.class.name
  end
end