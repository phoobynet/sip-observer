package database

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/phoobynet/sip-observer/config"
	"log"
)

type Database struct {
	ctx  context.Context
	conn *pgx.Conn
}

func NewDatabase(ctx context.Context, configuration *config.Config) *Database {
	pgConn, err := pgx.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@%s:%s/qdb", configuration.DBHost, configuration.DBPGPort))

	if err != nil {
		log.Fatal(err)
	}
	return &Database{
		ctx:  ctx,
		conn: pgConn,
	}
}

func (d *Database) DropTable(tableName string) {
	_, err := d.conn.Exec(d.ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	if err != nil {
		log.Fatal(err)
	}
}

func (d *Database) Close() {
	_ = d.conn.Close(d.ctx)
}
