package writer

import (
	"context"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"github.com/questdb/go-questdb-client"
	"github.com/samber/lo"
	"log"
	"sync"
	"time"
)

type TradeWriter struct {
	ctx              context.Context
	sender           *questdb.LineSender
	inputBuffer      []stream.Trade
	writeTicker      *time.Ticker
	logTicker        *time.Ticker
	writeLock        sync.RWMutex
	writeChan        chan []stream.Trade
	writtenCount     int64
	writtenCountLock sync.RWMutex
}

func NewTradeWriter(ctx context.Context, configuration *config.Config) (*TradeWriter, error) {
	sender, err := questdb.NewLineSender(ctx, questdb.WithAddress(configuration.DBHost))

	if err != nil {
		return nil, err
	}

	writeTicker := time.NewTicker(time.Second)
	writeChan := make(chan []stream.Trade, 10_000)

	logTicker := time.NewTicker(time.Second * 5)

	tradeWriter := &TradeWriter{
		ctx:         ctx,
		sender:      sender,
		writeTicker: writeTicker,
		writeChan:   writeChan,
		logTicker:   logTicker,
	}

	go func() {
		for {
			select {
			case <-writeTicker.C:
				tradeWriter.copyInputBuffer()
			case trades := <-writeChan:
				tradeWriter.flush(trades)
			case <-logTicker.C:
				tradeWriter.writtenCountLock.RLock()
				log.Printf("Written %d trades", tradeWriter.writtenCount)
				tradeWriter.writtenCountLock.RUnlock()
			}

		}
	}()

	return tradeWriter, nil
}

func (w *TradeWriter) Write(trade stream.Trade) {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	w.inputBuffer = append(w.inputBuffer, trade)
}

func (w *TradeWriter) Close() error {
	w.writeTicker.Stop()
	return w.sender.Close()
}

func (w *TradeWriter) copyInputBuffer() {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	tempBuffer := make([]stream.Trade, len(w.inputBuffer))
	copy(tempBuffer, w.inputBuffer)

	// Clear the input buffer
	w.inputBuffer = make([]stream.Trade, 0)

	// Send the buffer to the write channel
	w.writeChan <- tempBuffer
}

func (w *TradeWriter) flush(trades []stream.Trade) {
	var err error

	chunks := lo.Chunk(trades, 1_000)

	var c int64

	for _, chunkOfTrades := range chunks {
		for _, t := range chunkOfTrades {
			err = w.sender.Table("sip_observer_trades").
				Symbol("ticker", t.Symbol).
				Float64Column("price", t.Price).
				Float64Column("size", float64(t.Size)).
				TimestampColumn("trade_timestamp", t.Timestamp.UnixNano()).AtNow(w.ctx)

			if err != nil {
				log.Fatal(err)
			}

			c++
		}

		err = w.sender.Flush(w.ctx)

		if err != nil {
			log.Fatal(err)
		}
	}

	w.writtenCountLock.Lock()
	w.writtenCount += c
	defer w.writtenCountLock.Unlock()
}
