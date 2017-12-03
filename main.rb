require "active_support/all"
require "json"
require "kafka"
require "money"
require "rest-client"

require "./lib/robinhood"

Money.use_i18n = false

def format_money(amount)
  Money.new((amount.round(2) * 100).to_i, "USD").format
end

def buy_into(symbol)
  portfolio = Robinhood.new
  cash_for_buy = (portfolio.cash * 0.3).round(2)
  last_price = portfolio.last_price_for(symbol)

  return if last_price > cash_for_buy

  quantity = (cash_for_buy / last_price).floor.round

  puts "BUY #{quantity} x #{symbol} @ #{format_money(last_price)}"
  portfolio.market_buy(symbol, position["instrument"], quantity)
end

def sell_off(symbol)
  portfolio = Robinhood.new
  instrument = portfolio.instrument_for_symbol(symbol)
  instrument_id = instrument["id"]
  position = portfolio.position_for_instrument(instrument_id)
  last_price = portfolio.last_price_for(symbol)
  quantity = position["quantity"].to_f.round

  if quantity > 0
    puts "SELL #{quantity} x #{symbol} @ #{format_money(last_price)}"
    portfolio.market_sell(symbol, position["instrument"], quantity)
  end
end

logger = Logger.new(STDOUT)
logger.level = Logger::INFO

kafka = Kafka.new(
  connect_timeout: 30,
  logger: logger,
  seed_brokers: ["#{ENV["KAFKA_HOST"]}:#{ENV["KAFKA_PORT"]}"],
  client_id: "equity-trader"
)

consumer = kafka.consumer(group_id: "equity-trader")

begin
  consumer.subscribe("equity-signals", start_from_beginning: false)
rescue => e
  sleep 5
  retry
end

consumer.each_message do |message|
  symbol = message.key.upcase
  message = JSON.parse(message.value)
  signal = message["signal"]
  timestamp = message["at"]

  portfolio = Robinhood.new

  return if Time.parse(message["at"]) <= 1.day.ago

  begin
    case signal
      when "buy"
        buy_into(symbol)
      when "sell"
        sell_off(symbol)
    end
  rescue => e
    e.message
  end

  $stdout.flush
end
