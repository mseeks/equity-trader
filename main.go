package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/antonholmquist/jason"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	resty "gopkg.in/resty.v1"
)

type signalMessage struct {
	Value string `json:"signal"`
	At    string `json:"at"`
}

func buyInto(symbol string) error {
	resty.SetRedirectPolicy(resty.FlexibleRedirectPolicy(10))

	resp, err := resty.R().
		SetQueryParams(map[string]string{}).
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get("https://api.robinhood.com/accounts/")
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return err
	}

	results, err := value.GetObjectArray("results")
	if err != nil {
		return err
	}

	accountNumber, err := results[0].GetString("account_number")
	if err != nil {
		return err
	}

	buyingPower, err := results[0].GetString("buying_power")
	if err != nil {
		return err
	}

	buyingPowerAsDecimal, err := decimal.NewFromString(buyingPower)
	if err != nil {
		return err
	}

	cashForBuy := buyingPowerAsDecimal.Mul(decimal.NewFromFloat(0.3)).Round(2)

	fmt.Println(cashForBuy)

	resp, err = resty.R().
		SetQueryParams(map[string]string{}).
		SetHeader("Accept", "application/json").
		Get(fmt.Sprint("https://api.robinhood.com/quotes/", symbol, "/"))

	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err = jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return err
	}

	lastTradePrice, err := value.GetString("last_trade_price")
	if err != nil {
		return err
	}

	lastTradePriceAsDecimal, err := decimal.NewFromString(lastTradePrice)
	if err != nil {
		return err
	}

	if lastTradePriceAsDecimal.Cmp(cashForBuy) == 1 {
		return fmt.Errorf("Skipping order, not enough buying power")
	}

	count := cashForBuy.Div(lastTradePriceAsDecimal).Floor()
	fmt.Println("BUY", count, "x", symbol, "@", lastTradePriceAsDecimal.Round(2))

	resp, err = resty.R().
		SetQueryParams(map[string]string{
			"symbol": symbol,
		}).
		SetHeader("Accept", "application/json").
		Get("https://api.robinhood.com/instruments/")
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err = jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return err
	}

	results, err = value.GetObjectArray("results")
	if err != nil {
		return err
	}

	instrumentID, err := results[0].GetString("id")
	if err != nil {
		return err
	}

	resp, err = resty.R().
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get(fmt.Sprint("https://api.robinhood.com/positions/", accountNumber, "/", instrumentID, "/"))
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err = jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return err
	}

	ownedQuantity, err := value.GetString("quantity")
	if err != nil {
		return err
	}

	ownedQuantityAsDecimal, err := decimal.NewFromString(ownedQuantity)
	if err != nil {
		return err
	}

	if ownedQuantityAsDecimal.Cmp(decimal.NewFromFloat(0.0)) == 1 {
		return fmt.Errorf("Skipping order, already own equity")
	}

	return nil
}

func sellOff(symbol string) error {
	fmt.Println("SELL", symbol)
	return nil
}

func main() {
	broker := os.Getenv("KAFKA_ENDPOINT")
	topic := os.Getenv("KAFKA_TOPIC")
	partition, err := strconv.Atoi(os.Getenv("KAFKA_PARITION"))
	if err != nil {
		panic(err)
	}

	kafkaClientReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: partition,
	})
	defer kafkaClientReader.Close()

	err = kafkaClientReader.SetOffset(-2) // -2 is how you say you want the last offset
	if err != nil {
		panic(err)
	}

	for {
		message, err := kafkaClientReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
		}

		if message.Value != nil {
			symbol := string(message.Key)
			signal := signalMessage{}

			fmt.Println(symbol, "->", string(message.Value))

			err := json.Unmarshal(message.Value, &signal)
			if err != nil {
				fmt.Println(err)
				return
			}

			signaledAt, err := time.Parse("2006-01-02 15:04:05 -0700", signal.At)
			if err != nil {
				fmt.Println(err)
				return
			}

			yesterday := time.Now().UTC().Add(-24 * time.Hour).Unix()

			if signaledAt.Unix() > yesterday {
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
			} else {
				fmt.Println("Signal has expired, ignoring.")
			}
		}
	}
}
