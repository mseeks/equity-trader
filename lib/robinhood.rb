class Robinhood
  attr_accessor :active_orders, :api, :api_headers, :account, :account_number, :cash, :current_instruments, :orders, :positions

  def initialize
    @api = RestClient::Resource.new("https://api.robinhood.com")
    @api_headers = {
      accept: "application/json",
      Authorization: "Token #{ENV["ROBINHOOD_TOKEN"]}"
    }
  end

  def account
    @account ||= JSON.parse(@api["accounts/"].get(@api_headers).body)["results"].first
  end

  def account_number
    @account_number ||= account["account_number"]
  end

  def active_orders
    @active_orders ||= orders.select do |order|
      current_instruments.include?(order["instrument"]) && (order["state"] == "confirmed" || order["state"] == "queued")
    end
  end

  def cash
    @cash ||= if ENV["TEST_CASH"].nil? || ENV["TEST_CASH"].empty?
      account["buying_power"]
    else
      ENV["TEST_CASH"]
    end.to_f
  end

  def current_instruments
    @current_instruments ||= positions.map{|p| p["instrument"] }
  end

  def instrument_for_position(position)
    JSON.parse(RestClient.get(position["instrument"], @api_headers).body)
  end

  def instrument_for_symbol(symbol)
    unsecured_api_get("instruments/?symbol=#{symbol}")["results"].first
  end

  def position_for_instrument(instrument_id)
    secured_api_get("positions/#{self.account_number}/#{instrument_id}/")
  end

  def last_price_for(symbol)
    unsecured_api_get("quotes/#{symbol}/")["last_trade_price"].to_f
  end

  def market_buy(symbol, instrument, quantity)
    return unless ENV["TEST_CASH"].nil? || ENV["TEST_CASH"].empty?

    secured_api_post("orders/", {
      account: "https://api.robinhood.com/accounts/#{account_number}/",
      instrument: instrument,
      symbol: symbol,
      type: "market",
      trigger: "immediate",
      quantity: quantity,
      price: last_price_for(symbol).round(2),
      side: "buy",
      time_in_force: "gtc"
    })
  end

  def market_sell(symbol, instrument, quantity)
    return unless ENV["TEST_CASH"].nil? || ENV["TEST_CASH"].empty?

    secured_api_post("orders/", {
      account: "https://api.robinhood.com/accounts/#{account_number}/",
      instrument: instrument,
      symbol: symbol,
      type: "market",
      trigger: "immediate",
      quantity: quantity,
      side: "sell",
      time_in_force: "gtc"
    })
  end

  def open_orders_for(position)
    position_id_matcher = /[^\/]+(?=\/$|$)/

    active_orders.select do |order|
      first_id = position_id_matcher.match(order["position"])[0]
      second_id = position_id_matcher.match(position["url"])[0]

      first_id == second_id
    end
  end

  def orders
    @orders ||= secured_api_get("orders/")["results"]
  end

  def positions
    @positions ||= secured_api_get("accounts/#{account_number}/positions/?nonzero=true")["results"]
  end

  def secured_api_get(path)
    begin
      response = @api[path].get(@api_headers)
      JSON.parse(response.body)
    rescue => e
      puts e.message
      puts e.response.body
    end
  end

  def secured_api_post(path, params)
    begin
      response = @api[path].post(params, @api_headers)
      JSON.parse(response.body)
    rescue => e
      puts e.message
      puts e.response.body
    end
  end

  def unsecured_api_get(path)
    begin
      response = @api[path].get
      JSON.parse(response.body)
    rescue => e
      puts e.message
      puts e.response.body
    end
  end
end
