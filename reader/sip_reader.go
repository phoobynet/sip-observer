package reader

import (
	"context"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
)

type SIPReader struct {
	client        *stream.StocksClient
	configuration *config.Config
	ctx           context.Context
}

func NewSIPReader(ctx context.Context, configuration *config.Config) (*SIPReader, error) {
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
