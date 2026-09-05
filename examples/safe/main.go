package main

import (
	"context"
	firebirdotel "github.com/Makarechi/firebirdsql-otel"
	"log"
	"os"
)

func main() {
	cfg := firebirdotel.SafeConfig()
	cfg.Connection.Namespace = "billing"
	db, err := firebirdotel.OpenWithConfig(os.Getenv("FIREBIRD_DSN"), cfg)
	if err != nil {
		log.Fatal("open database failed")
	}
	defer db.Close()
	// Register once per pool. Unregister before closing the pool/providers.
	reg, err := firebirdotel.RegisterDBStatsMetricsWithConfig(db, cfg)
	if err != nil {
		log.Fatal("register pool metrics failed")
	}
	defer reg.Unregister()
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal("ping failed")
	}
}
