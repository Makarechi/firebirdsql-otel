package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"

	firebirdotel "github.com/Makarechi/firebirdsql-otel"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Initialize your application's OTel providers before registration.
	// Register once: the application keeps its existing database/sql setup.
	driverName, err := firebirdotel.Instrument()
	if err != nil {
		return err
	}
	db, err := sql.Open(driverName, os.Getenv("FIREBIRD_DSN"))
	if err != nil {
		return errors.New("open database failed")
	}
	defer db.Close()
	if err := db.PingContext(context.Background()); err != nil {
		return errors.New("ping failed")
	}
	return nil
}
