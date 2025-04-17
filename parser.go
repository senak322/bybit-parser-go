package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// JSON-ответ от Bybit
type BybitResponse struct {
	Result struct {
		Items []struct {
			Price string `json:"price"`
		} `json:"items"`
	} `json:"result"`
}

// Для сохранения в MongoDB
type Rate struct {
	FromCurrency string    `bson:"from_currency"`
	ToCurrency   string    `bson:"to_currency"`
	Amount       string    `bson:"amount"`
	PaymentID    string    `bson:"payment_id"`
	MedianPrice  float64   `bson:"median_price"`
	Time         time.Time `bson:"time"`
}

// Получение данных с учетом способа оплаты
func getOrders(currency, token, side, amount, paymentID string) ([]byte, error) {
	url := "https://api2.bybit.com/fiat/otc/item/online"

	payload := []byte(fmt.Sprintf(`{
		"tokenId": "%s",
		"currencyId": "%s",
		"side": "%s",
		"size": "10",
		"amount": "%s",
		"payment": ["%s"],
		"page": "1",
		"vaMaker": false,
		"bulkMaker": false,
		"canTrade": true,
		"verificationFilter": 0,
		"sortType": "TRADE_PRICE",
		"paymentPeriod": [],
		"itemRegion": 1
	}`, token, currency, side, amount, paymentID))

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// Медиана
func calculateMedian(prices []float64) float64 {
	sort.Float64s(prices)
	n := len(prices)
	if n%2 == 0 {
		return (prices[n/2-1] + prices[n/2]) / 2
	}
	return prices[n/2]
}

func main() {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(context.Background())

	collection := client.Database("bybit").Collection("rates")

	pairs := []struct {
		From      string
		To        string
		Side      string
		Amount    string
		PaymentID string
	}{
		{"RUB", "USDT", "1", "10000", "581"}, // RUB → USDT (СБП)
		{"GEL", "USDT", "1", "100", "29"},    // GEL → USDT (Bank of Georgia)
		{"USDT", "GEL", "0", "100", "29"},    // USDT → GEL (Bank of Georgia)
	}

	for _, pair := range pairs {
		body, err := getOrders(pair.From, pair.To, pair.Side, pair.Amount, pair.PaymentID)
		if err != nil {
			fmt.Println("Ошибка запроса:", err)
			continue
		}

		var response BybitResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			fmt.Println("Ошибка парсинга JSON:", err)
			continue
		}

		var prices []float64
		for i, item := range response.Result.Items {
			if i >= 1 && i < 10 {
				price, err := strconv.ParseFloat(item.Price, 64)
				if err == nil {
					prices = append(prices, price)
				}
			}
		}

		if len(prices) == 0 {
			fmt.Println("Нет данных для расчета медианы по паре", pair.From, "->", pair.To)
			continue
		}

		median := calculateMedian(prices)
		fmt.Printf("Медианный курс %s -> %s: %.2f\n", pair.From, pair.To, median)

		rate := Rate{
			FromCurrency: pair.From,
			ToCurrency:   pair.To,
			Amount:       pair.Amount,
			PaymentID:    pair.PaymentID,
			MedianPrice:  median,
			Time:         time.Now(),
		}

		_, err = collection.InsertOne(context.Background(), rate)
		if err != nil {
			fmt.Println("Ошибка при записи в MongoDB:", err)
		} else {
			fmt.Println("Курс успешно сохранен в MongoDB")
		}
	}
}
