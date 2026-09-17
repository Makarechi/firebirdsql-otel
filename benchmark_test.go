package firebirdotel

import (
	"context"
	"database/sql"
	"testing"
)

func BenchmarkClient(b *testing.B) {
	for _, mode := range []string{"plain", "compatibility", "safe", "diagnostic"} {
		b.Run(mode, func(b *testing.B) {
			var db *sql.DB
			var err error
			switch mode {
			case "plain":
				db = sql.OpenDB(mockConnector{})
			case "compatibility":
				db = OpenDB(mockConnector{})
			case "safe":
				db, err = OpenDBWithConfig(mockConnector{}, SafeConfig())
			case "diagnostic":
				db, err = OpenDBWithConfig(mockConnector{}, DiagnosticConfig())
			}
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.ExecContext(ctx, "execute procedure work(?, 'synthetic')", 42); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
