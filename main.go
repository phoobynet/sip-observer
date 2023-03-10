package main

import (
	"context"
	"errors"
	"flag"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/phoobynet/sip-observer/assets"
	"github.com/phoobynet/sip-observer/config"
	"github.com/phoobynet/sip-observer/reader"
	"github.com/phoobynet/sip-observer/snapshots"
	"github.com/phoobynet/sip-observer/writer"
	"log"
	"os"
	"os/signal"
)

var configurationFile string

func main() {
	if key := os.Getenv("APCA_API_KEY_ID"); key == "" {
		log.Fatal(errors.New("APCA_API_KEY_ID is not set"))

	}

	if secret := os.Getenv("APCA_API_SECRET_KEY"); secret == "" {
		log.Fatal(errors.New("APCA_API_SECRET_KEY is not set"))
	}

	flag.StringVar(&configurationFile, "config", "config.toml", "Configuration file")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	configuration, err := config.Load(configurationFile)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Configuration: %+v\n\n", configuration)

	allSymbols := assets.Load(context.TODO(), configuration)

	snapshots.Load(context.TODO(), configuration, allSymbols)

	readerCtx, readerCancel := context.WithCancel(context.Background())

	sipReader, err := reader.NewSIPReader(readerCtx, configuration)

	if err != nil {
		log.Fatal(err)
	}

	writerCtx, writerCancel := context.WithCancel(context.Background())

	sipWriter, err := writer.NewSIPWriter(writerCtx, configuration)

	if err != nil {
		log.Fatal(err)
	}

	var streamingTradesChan = make(chan stream.Trade, 100_000)
	var streamingBarsChan = make(chan stream.Bar, 20_000)

	go func() {
		err := sipReader.Observe(streamingTradesChan, streamingBarsChan)

		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("SIP observer started")
		}

		for {
			select {
			case t := <-streamingTradesChan:
				sipWriter.WriteTrade(t)
			case b := <-streamingBarsChan:
				sipWriter.WriteBar(b)
			case <-readerCtx.Done():
				log.Println("Shutting down SIP Observer reader")
				_ = sipReader.Disconnect()
				return
			case <-writerCtx.Done():
				log.Println("Shutting down SIP Observer writer")
				_ = sipWriter.Close()
				return
			}
		}
	}()

	<-quit
	readerCancel()
	writerCancel()
}
