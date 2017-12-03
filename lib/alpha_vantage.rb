class AlphaVantage
  attr_accessor :api, :api_key, :macd, :macd_query, :signal, :symbol

  def initialize(symbol)
    @symbol = symbol.upcase
    @api = RestClient::Resource.new("https://www.alphavantage.co")
    @api_key = ENV["ALPHAVANTAGE_API_KEY"]
  end

  def macd_query
    @macd_query ||= JSON.parse(@api["query"].get(
      {
        params: {
          function: "MACD",
          symbol: @symbol,
          interval: "daily",
          series_type: "close",
          apikey: @api_key
        }
      }
    ).body)["Technical Analysis: MACD"].first.last
  end

  def macd
    @macd ||= macd_query["MACD"].to_f
  end

  def signal
    @signal ||= macd_query["MACD_Signal"].to_f
  end
end
