package reader

import (
	"context"
	"errors"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"os"
)

type SIPReader struct {
	client        *stream.StocksClient
	configuration *config.Config
	ctx           context.Context
}

func NewSIPReader(ctx context.Context, configuration *config.Config) (*SIPReader, error) {
	if key := os.Getenv("APCA_API_KEY_ID"); key == "" {
		return nil, errors.New("APCA_API_KEY_ID is not set")

	}

	if secret := os.Getenv("APCA_API_SECRET_KEY"); secret == "" {
		return nil, errors.New("APCA_API_SECRET_KEY is not set")
	}

	client := stream.NewStocksClient(marketdata.SIP)

	err := client.Connect(ctx)

	if err != nil {
		return nil, err
	}

	return &SIPReader{
		client,
		configuration,
		ctx,
	}, nil
}

func (r *SIPReader) Observe(streamingTradesChan chan stream.Trade, streamingBarsChan chan stream.Bar) error {
	err := r.client.SubscribeToTrades(func(t stream.Trade) {
		streamingTradesChan <- t
	}, r.configuration.Symbols...)

	if err != nil {
		return err
	}

	return r.client.SubscribeToBars(func(b stream.Bar) {
		streamingBarsChan <- b
	}, r.configuration.Symbols...)
}

func (r *SIPReader) Disconnect() error {
	err := r.client.UnsubscribeFromTrades(r.configuration.Symbols...)

	if err != nil {
		return err
	}

	return r.client.UnsubscribeFromBars(r.configuration.Symbols...)
}
