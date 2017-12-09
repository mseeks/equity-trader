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

  instrument = portfolio.instrument_for_symbol(symbol)
  position = portfolio.position_for_instrument(instrument["id"])
  owned_quantity = position["quantity"].to_f.round
  quantity = (cash_for_buy / last_price).floor.round

  # Check to make sure we don't already have a stake in that equity
  unless owned_quantity > 0
    puts "BUY #{quantity} x #{symbol} @ #{format_money(last_price)}"
    portfolio.market_buy(symbol, position["instrument"], quantity)
  end
end

def sell_off(symbol)
  portfolio = Robinhood.new
  instrument = portfolio.instrument_for_symbol(symbol)
  instrument_id = instrument["id"]
  position = portfolio.position_for_instrument(instrument_id)
  last_price = portfolio.last_price_for(symbol)
  quantity = position["quantity"].to_f.round

  # Check to make sure we have a stake in that equity
  if quantity > 0
    puts "SELL #{quantity} x #{symbol} @ #{format_money(last_price)}"
    portfolio.market_sell(symbol, position["instrument"], quantity)
  end
end

logger = Logger.new(STDOUT)
logger.level = Logger::WARN

kafka = Kafka.new(
  logger: logger,
  seed_brokers: ["#{ENV["KAFKA_HOST"]}:#{ENV["KAFKA_PORT"]}"],
  client_id: "equity-trader"
)

consumer = kafka.consumer(group_id: "equity-trader")

begin
  consumer.subscribe("equity-signals", start_from_beginning: false)

  consumer.each_message(automatically_mark_as_processed: false) do |message|
    symbol = message.key.upcase
    message = JSON.parse(message.value)
    signal = message["signal"]
    timestamp = message["at"]

    portfolio = Robinhood.new

    unless Time.parse(message["at"]) <= 1.day.ago
      begin
        case signal
          when "buy"
            buy_into(symbol)
          when "sell"
            sell_off(symbol)
        end

        consumer.mark_message_as_processed(message)
      rescue => e
        puts e.message
      end
    end

    $stdout.flush
  end
rescue => e
  puts e.message
  sleep 5
  retry
end
