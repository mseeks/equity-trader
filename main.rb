require "active_record"
require "json"
require "kafka"
require "pg"
require "rest-client"

require "./lib/alpha_vantage"

db_config = {
  host:     ENV["DB_HOST"],
  adapter:  "postgresql",
  encoding: "utf-8",
  database: ENV["DB_NAME"],
  username: ENV["DB_USERNAME"],
  password: ENV["DB_PASSWORD"]
}

ActiveRecord::Base.establish_connection(db_config)
ActiveRecord::Migrator.migrate("db/migrate/")

kafka = Kafka.new(
  seed_brokers: ["#{ENV["KAFKA_HOST"]}:#{ENV["KAFKA_PORT"]}"],
  client_id: "equity-signaler"
)

class Equity < ActiveRecord::Base
  enum signal: {
    sell: 0, # default
    buy: 1
  }

  # Decide if buy or sell and initiate signal update
  def update_signal!
    result = AlphaVantage.new(self.symbol)

    if result.macd > result.signal
      self.signal!(:buy)
    else
      self.signal!(:sell)
    end
  end

  # Updates the signal in the DB and send a message to Kafka if it changed
  def signal!(type)
    self.signal = type
    self.send_message! if self.signal_changed? && self.save
  end

  # Send a message to Kafka about the signal change
  def send_message!
    message = {
      signal: self.signal,
      at: Time.now
    }.to_json

    puts "#{self.symbol} -> #{message}"
    kafka.deliver_message(message, topic: "equity_signals", key: self.symbol)
  end
end

potential_securities = ENV["POTENTIAL_SECURITIES"].split(",")

potential_securities.each do |potential_security|
  begin
    @equity = Equity.where(symbol: potential_security.upcase).first_or_create
    @equity.update_signal!
  rescue => e
    puts e.message
  end

  $stdout.flush
end
