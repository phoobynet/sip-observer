package main

import (
	"context"
	"flag"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/config"
	"github.com/phoobynet/sip-observer/reader"
	"github.com/phoobynet/sip-observer/writer"
	"log"
	"os"
	"os/signal"
)

var configurationFile string

func main() {
	flag.StringVar(&configurationFile, "config", "config.toml", "Configuration file")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	configuration, err := config.Load(configurationFile)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Configuration: %+v\n\n", configuration)

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
				_ = tradeReader.Disconnect()
				return
			case <-writerCtx.Done():
				log.Println("Shutting down sip-observer writer")
				_ = tradeWriter.Close()
				return
			}
		}
	}()

	<-quit
	readerCancel()
	writerCancel()
}
