package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/job"
	"github.com/example/order-book-imbalance/internal/modules"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	dbAdapter := modules.NewPostgresRelationalDB(cfg.DB)
	db, err := dbAdapter.Connect(context.Background())
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer db.Close()

	store := modules.NewPostgresMarketStore(db)
	stockClient := modules.NewSSIStockClient(cfg.SSI)

	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Println("migration OK")

	log.Println("running MarketDataJob...")
	ict, err := time.LoadLocation(cfg.ATO.Timezone)
	if err != nil {
		log.Fatalf("load timezone: %v", err)
	}
	j := job.NewMarketDataJob(stockClient, store, cfg.Signal, ict)
	j.Run()
	log.Println("job complete")

	ctx := context.Background()

	tables := []struct {
		name  string
		query string
	}{
		{"stock_ohlcv", "SELECT COUNT(*) FROM stock_ohlcv"},
		{"stock_foreign_flow", "SELECT COUNT(*) FROM stock_foreign_flow"},
		{"index_ohlcv", "SELECT COUNT(*) FROM index_ohlcv"},
		{"stock_metrics", "SELECT COUNT(*) FROM stock_metrics"},
		{"market_regime", "SELECT COUNT(*) FROM market_regime"},
	}

	fmt.Println()
	fmt.Println("--- row counts ---")
	for _, t := range tables {
		var n int64
		if err := db.QueryRowContext(ctx, t.query).Scan(&n); err != nil {
			log.Printf("  %s: ERROR %v", t.name, err)
			continue
		}
		fmt.Printf("  %-25s %d\n", t.name, n)
	}

	// Spot-check: latest trading date per table
	fmt.Println()
	fmt.Println("--- latest trading dates ---")
	dateQueries := []struct {
		name  string
		query string
	}{
		{"stock_ohlcv", "SELECT MAX(trading_date) FROM stock_ohlcv"},
		{"stock_foreign_flow", "SELECT MAX(trading_date) FROM stock_foreign_flow"},
		{"index_ohlcv", "SELECT MAX(trading_date) FROM index_ohlcv"},
		{"stock_metrics", "SELECT MAX(trading_date) FROM stock_metrics"},
		{"market_regime", "SELECT MAX(trading_date) FROM market_regime"},
	}
	for _, q := range dateQueries {
		var date *string
		if err := db.QueryRowContext(ctx, q.query).Scan(&date); err != nil {
			log.Printf("  %s: ERROR %v", q.name, err)
			continue
		}
		d := "<empty>"
		if date != nil {
			d = *date
		}
		fmt.Printf("  %-25s %s\n", q.name, d)
	}

	// Spot-check: latest market regime
	fmt.Println()
	regime, hasRegime, err := store.LoadLatestMarketRegime(ctx)
	if err != nil {
		log.Printf("LoadLatestMarketRegime: %v", err)
	} else if hasRegime {
		fmt.Printf("market_regime: %s on %s\n", regime.Regime, regime.TradingDate)
	} else {
		fmt.Println("market_regime: no rows")
	}

	// Spot-check: sample 5 stock_metrics rows
	rows, err := db.QueryContext(ctx, `
		SELECT symbol, TO_CHAR(trading_date, 'DD/MM/YYYY'), final_score, position_size_flag, should_monitor_today
		FROM stock_metrics
		ORDER BY final_score DESC
		LIMIT 5
	`)
	if err != nil {
		log.Printf("sample stock_metrics: %v", err)
		return
	}
	defer rows.Close()
	fmt.Println()
	fmt.Println("--- top 5 stock_metrics by final_score ---")
	for rows.Next() {
		var sym, date, flag string
		var score int
		var monitor bool
		if err := rows.Scan(&sym, &date, &score, &flag, &monitor); err != nil {
			log.Printf("scan: %v", err)
			continue
		}
		fmt.Printf("  %-8s %s  score=%-3d  flag=%-5s  monitor=%v\n", sym, date, score, flag, monitor)
	}
}
