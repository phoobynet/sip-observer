package writer

import (
	"context"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"github.com/questdb/go-questdb-client"
	"log"
	"sync"
	"time"
)

type TradeWriter struct {
	ctx         context.Context
	sender      *questdb.LineSender
	buffer      []stream.Trade
	writeTicker *time.Ticker
	writeLock   sync.RWMutex
}

func NewTradeWriter(ctx context.Context, configuration *config.Config) (*TradeWriter, error) {
	sender, err := questdb.NewLineSender(ctx, questdb.WithAddress(configuration.DBHost))

	if err != nil {
		return nil, err
	}

	buffer := make([]stream.Trade, 0)

	writeTicker := time.NewTicker(time.Second)

	tradeWriter := &TradeWriter{ctx: ctx, sender: sender, buffer: buffer, writeTicker: writeTicker}

	go func() {
		for range writeTicker.C {
			tradeWriter.writeBuffer()
		}
	}()

	return tradeWriter, nil
}

func (w *TradeWriter) Write(trade stream.Trade) {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	w.buffer = append(w.buffer, trade)
}

func (w *TradeWriter) Close() error {
	w.writeTicker.Stop()
	return w.sender.Close()
}

func (w *TradeWriter) writeBuffer() {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	tempBuffer := make([]stream.Trade, len(w.buffer))
	copy(tempBuffer, w.buffer)
	w.buffer = make([]stream.Trade, 0)

	var err error
	for _, trade := range tempBuffer {
		err = w.sender.Table("trades").
			Symbol("symbol", trade.Symbol).
			Float64Column("price", trade.Price).
			Float64Column("size", float64(trade.Size)).
			TimestampColumn("t", trade.Timestamp.UnixNano()).AtNow(w.ctx)

		if err != nil {
			log.Fatal(err)
		}
	}
}
