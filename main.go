package main

import (
	"context"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"github.com/phoobynet/sip-observer/reader"
	"github.com/phoobynet/sip-observer/writer"
	"log"
	"os"
	"os/signal"
)

func main() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	configuration, err := config.Load("config.toml")

	if err != nil {
		log.Fatal(err)
	}

	readerCtx, readerCancel := context.WithCancel(context.Background())

	tradeReader, err := reader.NewTradeReader(readerCtx, configuration)

	if err != nil {
		log.Fatal(err)
	}

	writerCtx, writerCancel := context.WithCancel(context.Background())

	tradeWriter, err := writer.NewTradeWriter(writerCtx, configuration)

	if err != nil {
		log.Fatal(err)
	}

	var streamingTradesChan = make(chan stream.Trade, 100_000)

	go func() {
		err := tradeReader.Observe(streamingTradesChan)

		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("Trade reader started")
		}

		for {
			select {
			case t := <-streamingTradesChan:
				tradeWriter.Write(t)
			case <-readerCtx.Done():
				log.Println("Shutting down sip-observer reader")
				return
			case <-writerCtx.Done():
				log.Println("Shutting down sip-observer writer")
				return
			}
		}
	}()

	<-quit
	readerCancel()
	writerCancel()
	//os.Exit(0)
}
