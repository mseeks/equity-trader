package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/antonholmquist/jason"
	"github.com/shopspring/decimal"
	"gopkg.in/resty.v1"
)

type signalMessage struct {
	Value string `json:"signal"`
	At    string `json:"at"`
}

func accountMetaInformation() (string, decimal.Decimal, error) {
	resp, err := resty.R().
		SetQueryParams(map[string]string{}).
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get("https://api.robinhood.com/accounts/")
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	if resp.StatusCode() != 200 {
		return "", decimal.Decimal{}, fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	results, err := value.GetObjectArray("results")
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	accountNumber, err := results[0].GetString("account_number")
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	buyingPower, err := results[0].GetString("buying_power")
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	buyingPowerAsDecimal, err := decimal.NewFromString(buyingPower)
	if err != nil {
		return "", decimal.Decimal{}, err
	}

	return accountNumber, buyingPowerAsDecimal, nil
}

func instrumentIDFromSymbol(symbol string) (string, error) {
	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"symbol": symbol,
		}).
		SetHeader("Accept", "application/json").
		Get("https://api.robinhood.com/instruments/")
	if err != nil {
		return "", err
	}

	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return "", err
	}

	results, err := value.GetObjectArray("results")
	if err != nil {
		return "", err
	}

	instrumentID, err := results[0].GetString("id")
	if err != nil {
		return "", err
	}

	return instrumentID, nil
}

func positionMetaInformation(accountNumber string, instrumentID string) (decimal.Decimal, string, error) {
	resp, err := resty.R().
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get(fmt.Sprint("https://api.robinhood.com/positions/", accountNumber, "/", instrumentID, "/"))
	if err != nil {
		return decimal.Decimal{}, "", err
	}

	if resp.StatusCode() != 200 {
		return decimal.Decimal{}, "", fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return decimal.Decimal{}, "", err
	}

	instrumentURL, err := value.GetString("instrument")
	if err != nil {
		return decimal.Decimal{}, "", err
	}

	ownedQuantity, err := value.GetString("quantity")
	if err != nil {
		return decimal.Decimal{}, "", err
	}

	ownedQuantityAsDecimal, err := decimal.NewFromString(ownedQuantity)
	if err != nil {
		return decimal.Decimal{}, "", err
	}

	return ownedQuantityAsDecimal, instrumentURL, nil
}

func lastTradePriceForSymbol(symbol string) (decimal.Decimal, error) {
	resp, err := resty.R().
		SetQueryParams(map[string]string{}).
		SetHeader("Accept", "application/json").
		Get(fmt.Sprint("https://api.robinhood.com/quotes/", symbol, "/"))

	if err != nil {
		return decimal.Decimal{}, err
	}

	if resp.StatusCode() != 200 {
		return decimal.Decimal{}, fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return decimal.Decimal{}, err
	}

	lastTradePrice, err := value.GetString("last_trade_price")
	if err != nil {
		return decimal.Decimal{}, err
	}

	lastTradePriceAsDecimal, err := decimal.NewFromString(lastTradePrice)
	if err != nil {
		return decimal.Decimal{}, err
	}

	return lastTradePriceAsDecimal, nil
}

func buyInto(symbol string) error {
	accountNumber, buyingPower, err := accountMetaInformation()
	if err != nil {
		return err
	}

	cashForBuy := buyingPower.Mul(decimal.NewFromFloat(0.3)).Round(2)

	lastTradePrice, err := lastTradePriceForSymbol(symbol)
	if err != nil {
		return err
	}

	if lastTradePrice.Cmp(cashForBuy) == 1 {
		return fmt.Errorf("Skipping order, not enough buying power")
	}

	instrumentID, err := instrumentIDFromSymbol(symbol)
	if err != nil {
		return err
	}

	ownedQuantity, instrumentURL, err := positionMetaInformation(accountNumber, instrumentID)
	if err != nil {
		return err
	}

	if ownedQuantity.Cmp(decimal.NewFromFloat(0.0)) == 1 {
		return fmt.Errorf("Skipping order, already own equity")
	}

	count := cashForBuy.Div(lastTradePrice).Floor()
	fmt.Println("BUY", count, "x", symbol, "@", lastTradePrice.Round(2))

	resp, err := resty.R().
		SetBody(map[string]interface{}{
			"account":       fmt.Sprint("https://api.robinhood.com/accounts/", accountNumber, "/"),
			"instrument":    instrumentURL,
			"symbol":        symbol,
			"type":          "market",
			"trigger":       "immediate",
			"quantity":      count,
			"price":         lastTradePrice.Round(2),
			"side":          "buy",
			"time_in_force": "gtc",
		}).
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Post("https://api.robinhood.com/orders/")
	if err != nil {
		return err
	}

	if resp.StatusCode() != 201 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	return nil
}

func sellOff(symbol string) error {
	accountNumber, _, err := accountMetaInformation()
	if err != nil {
		return err
	}

	lastTradePrice, err := lastTradePriceForSymbol(symbol)
	if err != nil {
		return err
	}

	instrumentID, err := instrumentIDFromSymbol(symbol)
	if err != nil {
		return err
	}

	ownedQuantity, instrumentURL, err := positionMetaInformation(accountNumber, instrumentID)
	if err != nil {
		return err
	}

	if ownedQuantity.Cmp(decimal.NewFromFloat(0.0)) != 1 {
		return fmt.Errorf("Skipping order, don't own equity")
	}

	count := ownedQuantity.Round(0)
	fmt.Println("SELL", count, "x", symbol, "@", lastTradePrice.Round(2))

	resp, err := resty.R().
		SetBody(map[string]interface{}{
			"account":       fmt.Sprint("https://api.robinhood.com/accounts/", accountNumber, "/"),
			"instrument":    instrumentURL,
			"symbol":        symbol,
			"type":          "market",
			"trigger":       "immediate",
			"quantity":      ownedQuantity,
			"side":          "sell",
			"time_in_force": "gtc",
		}).
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Post("https://api.robinhood.com/orders/")
	if err != nil {
		return err
	}

	if resp.StatusCode() != 201 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	return nil
}

func main() {
	resty.SetRedirectPolicy(resty.FlexibleRedirectPolicy(10))

	broker := os.Getenv("KAFKA_ENDPOINT")
	topic := os.Getenv("KAFKA_CONSUMER_TOPIC")

	for {
		consumer, err := sarama.NewConsumer([]string{broker}, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer partitionConsumer.Close()

		for {
			select {
			case message := <-partitionConsumer.Messages():
				symbol := string(message.Key)
				signal := signalMessage{}

				fmt.Println("Received:", symbol, "->", string(message.Value))

				err = json.Unmarshal(message.Value, &signal)
				if err != nil {
					fmt.Println(err)
					continue
				}

				signaledAt, err := time.Parse("2006-01-02 15:04:05 -0700", signal.At)
				if err != nil {
					fmt.Println(err)
					continue
				}

				yesterday := time.Now().UTC().Add(-24 * time.Hour).Unix()

				if signaledAt.Unix() < yesterday {
					fmt.Println("Signal has expired, ignoring.")
					continue
				}

				if strings.ToLower(signal.Value) == "buy" {
					err := buyInto(symbol)
					if err != nil {
						fmt.Println(err)
					}
				} else if strings.ToLower(signal.Value) == "sell" {
					err := sellOff(symbol)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}
}
