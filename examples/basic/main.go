package main

import (
	"context"
	"fmt"
	"log"
	"os"

	firebirdotel "github.com/Makarechi/firebirdsql-otel"
)

func main() {
	dsn := os.Getenv("FIREBIRD_DSN")
	if dsn == "" {
		fmt.Println("set FIREBIRD_DSN to run the example")
		return
	}

	db, err := firebirdotel.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(context.Background(), "execute procedure recalculate_invoice(?)", 42); err != nil {
		log.Fatal(err)
	}
}
