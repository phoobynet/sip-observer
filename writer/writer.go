package writer

import (
	"context"
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"github.com/questdb/go-questdb-client"
	"github.com/samber/lo"
	"log"
	"sync"
	"time"
)

type SIPWriter struct {
	ctx                   context.Context
	sender                *questdb.LineSender
	logTicker             *time.Ticker
	tradeInputBuffer      []stream.Trade
	tradeWriteTicker      *time.Ticker
	tradeWriteLock        sync.RWMutex
	tradeWriteChan        chan []stream.Trade
	tradeWrittenCount     int64
	tradeWrittenCountLock sync.RWMutex
	barInputBuffer        []stream.Bar
	barWriteTicker        *time.Ticker
	barWriteLock          sync.RWMutex
	barWriteChan          chan []stream.Bar
	barWrittenCount       int64
	barWrittenCountLock   sync.RWMutex
}

func NewSIPWriter(ctx context.Context, configuration *config.Config) (*SIPWriter, error) {
	sender, err := questdb.NewLineSender(ctx, questdb.WithAddress(fmt.Sprintf("%s:%s", configuration.DBHost, configuration.DBILPPort)))

	if err != nil {
		return nil, err
	}

	tradeWriteTicker := time.NewTicker(time.Second)
	tradeWriteChan := make(chan []stream.Trade, 10_000)
	barWriteTicker := time.NewTicker(5 * time.Second)
	barWriteChan := make(chan []stream.Bar, 10_000)

	logTicker := time.NewTicker(time.Second * 5)

	sipWriter := &SIPWriter{
		ctx:              ctx,
		sender:           sender,
		logTicker:        logTicker,
		tradeWriteTicker: tradeWriteTicker,
		tradeWriteChan:   tradeWriteChan,
		barWriteTicker:   barWriteTicker,
		barWriteChan:     barWriteChan,
	}

	go func() {
		for {
			select {
			case <-tradeWriteTicker.C:
				sipWriter.copyTradeInputBuffer()
			case <-barWriteTicker.C:
				sipWriter.copyBarInputBuffer()
			case trades := <-tradeWriteChan:
				sipWriter.flushTrades(trades)
			case bars := <-barWriteChan:
				sipWriter.flushBars(bars)
			case <-logTicker.C:
				sipWriter.tradeWrittenCountLock.RLock()
				sipWriter.barWrittenCountLock.RLock()
				log.Printf("Trades: %d / Bars: %d", sipWriter.tradeWrittenCount, sipWriter.barWrittenCount)
				sipWriter.tradeWrittenCountLock.RUnlock()
				sipWriter.barWrittenCountLock.RUnlock()
			}
		}
	}()

	return sipWriter, nil
}

func (s *SIPWriter) WriteTrade(trade stream.Trade) {
	s.tradeWriteLock.Lock()
	defer s.tradeWriteLock.Unlock()

	s.tradeInputBuffer = append(s.tradeInputBuffer, trade)
}

func (s *SIPWriter) WriteBar(bar stream.Bar) {
	s.barWriteLock.Lock()
	defer s.barWriteLock.Unlock()

	s.barInputBuffer = append(s.barInputBuffer, bar)
}

func (s *SIPWriter) Close() error {
	s.tradeWriteTicker.Stop()
	s.barWriteTicker.Stop()

	return s.sender.Close()
}

func (s *SIPWriter) copyTradeInputBuffer() {
	s.tradeWriteLock.Lock()
	defer s.tradeWriteLock.Unlock()

	tempBuffer := make([]stream.Trade, len(s.tradeInputBuffer))
	copy(tempBuffer, s.tradeInputBuffer)

	// Clear the input buffer
	s.tradeInputBuffer = make([]stream.Trade, 0)

	// Send the buffer to the write channel
	s.tradeWriteChan <- tempBuffer
}

func (s *SIPWriter) copyBarInputBuffer() {
	s.barWriteLock.Lock()
	defer s.barWriteLock.Unlock()

	tempBuffer := make([]stream.Bar, len(s.barInputBuffer))
	copy(tempBuffer, s.barInputBuffer)

	// Clear the input buffer
	s.barInputBuffer = make([]stream.Bar, 0)

	// Send the buffer to the write channel
	s.barWriteChan <- tempBuffer
}

func (s *SIPWriter) flushTrades(trades []stream.Trade) {
	var err error

	chunks := lo.Chunk(trades, 1_000)

	var c int64

	for _, chunkOfTrades := range chunks {
		for _, t := range chunkOfTrades {
			err = s.sender.Table("sip_observer_trades").
				Symbol("ticker", t.Symbol).
				Float64Column("price", t.Price).
				Float64Column("size", float64(t.Size)).
				TimestampColumn("trade_timestamp", t.Timestamp.UnixMicro()).AtNow(s.ctx)

			if err != nil {
				log.Fatal(err)
			}

			c++
		}

		err = s.sender.Flush(s.ctx)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.tradeWrittenCountLock.Lock()
	s.tradeWrittenCount += c
	defer s.tradeWrittenCountLock.Unlock()
}

func (s *SIPWriter) flushBars(bars []stream.Bar) {
	var err error

	chunks := lo.Chunk(bars, 1_000)

	var c int64

	for _, chunkOfBars := range chunks {
		for _, b := range chunkOfBars {
			err = s.sender.Table("sip_observer_bars").
				Symbol("ticker", b.Symbol).
				Float64Column("o", b.Open).
				Float64Column("h", b.High).
				Float64Column("l", b.Low).
				Float64Column("c", b.Close).
				Float64Column("v", float64(b.Volume)).
				Float64Column("vw", b.VWAP).
				Float64Column("n", float64(b.TradeCount)).
				TimestampColumn("bar_timestamp", b.Timestamp.UnixMicro()).AtNow(s.ctx)

			if err != nil {
				log.Fatal(err)
			}

			c++
		}

		err = s.sender.Flush(s.ctx)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.barWrittenCountLock.Lock()
	s.barWrittenCount += c
	defer s.barWrittenCountLock.Unlock()
}
