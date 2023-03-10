package reader

import (
	"context"
	"errors"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"os"
)

type TradeReader struct {
	key           string
	secret        string
	baseURL       string
	client        *stream.StocksClient
	configuration *config.Config
	ctx           context.Context
}

func NewTradeReader(ctx context.Context, configuration *config.Config) (*TradeReader, error) {
	key := os.Getenv("APCA_API_KEY_ID")

	if key == "" {
		return nil, errors.New("APCA_API_KEY_ID is not set")
	}

	secret := os.Getenv("APCA_API_SECRET_KEY")

	if secret == "" {
		return nil, errors.New("APCA_API_SECRET_KEY is not set")
	}

	baseURL := os.Getenv("APCA_API_BASE_URL")

	if baseURL == "" {
		return nil, errors.New("APCA_API_BASE_URL is not set")
	}

	client := stream.NewStocksClient(marketdata.SIP)

	err := client.Connect(ctx)

	if err != nil {
		return nil, err
	}

	return &TradeReader{
		key,
		secret,
		baseURL,
		client,
		configuration,
		ctx,
	}, nil
}

func (r *TradeReader) Observe(streamingTradesChan chan stream.Trade) error {
	return r.client.SubscribeToTrades(func(t stream.Trade) {
		streamingTradesChan <- t
	}, r.configuration.Symbols...)
}

func (r *TradeReader) Disconnect() error {
	err := r.client.UnsubscribeFromTrades(r.configuration.Symbols...)

	if err != nil {
		return err
	}

	return nil
}
