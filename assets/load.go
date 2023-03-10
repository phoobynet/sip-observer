package assets

import (
	"context"
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/phoobynet/sip-observer/config"
	"github.com/phoobynet/sip-observer/database"
	"github.com/questdb/go-questdb-client"
	"github.com/samber/lo"
	"log"
)

func Load(ctx context.Context, configuration *config.Config) []string {
	log.Println("Loading assets...")
	db := database.NewDatabase(ctx, configuration)
	db.DropTable("sip_observer_assets")

	defer func(db *database.Database) {
		db.Close()
	}(db)

	sender, err := questdb.NewLineSender(ctx, questdb.WithAddress(fmt.Sprintf("%s:%s", configuration.DBHost, configuration.DBILPPort)))

	defer func(sender *questdb.LineSender) {
		_ = sender.Close()
	}(sender)

	if err != nil {
		log.Fatal(err)
	}

	client := alpaca.NewClient(alpaca.ClientOpts{})

	assets, err := client.GetAssets(alpaca.GetAssetsRequest{
		Status:     "active",
		AssetClass: "us_equity",
	})

	if err != nil {
		log.Fatal(err)
	}

	symbols := make([]string, 0)
	assetChunks := lo.Chunk(assets, 1_000)

	for _, assets := range assetChunks {
		for _, asset := range assets {
			err := sender.Table("sip_observer_assets").
				Symbol("ticker", asset.Symbol).
				StringColumn("name", asset.Name).
				StringColumn("exchange", asset.Exchange).
				AtNow(ctx)

			if err != nil {
				log.Fatal(err)
			}

			symbols = append(symbols, asset.Symbol)
		}
	}

	log.Println("Loading assets...DONE")

	return symbols
}
