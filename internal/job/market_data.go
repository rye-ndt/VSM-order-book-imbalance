package job

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/example/order-book-imbalance/internal/calculator"
	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

const (
	fetchTimeout    = 2 * time.Hour
	defaultLookback = 20 * 24 * time.Hour
	vnIndexSymbol   = "VNINDEX"
	dateLayout      = "2006-01-02"

	stockHistoryDays = 30
	indexHistoryDays = 150
)

type MarketDataJob struct {
	client input.StockDataClient
	store  output.MarketStore
	signal config.SignalConfig
}

func NewMarketDataJob(
	client input.StockDataClient,
	store output.MarketStore,
	signal config.SignalConfig,
) *MarketDataJob {
	return &MarketDataJob{client: client, store: store, signal: signal}
}

func (j *MarketDataJob) Run() {
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	today := truncateDay(time.Now())
	yesterday := today.AddDate(0, 0, -1)
	firstRunFrom := today.Add(-defaultLookback)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); j.syncStockOHLCV(ctx, yesterday, firstRunFrom, today) }()
	go func() { defer wg.Done(); j.syncForeignFlow(ctx, yesterday, firstRunFrom, today) }()
	go func() { defer wg.Done(); j.syncIndexOHLCV(ctx, yesterday, firstRunFrom, today) }()
	wg.Wait()

	j.runMetricsPipeline(ctx)
}

func (j *MarketDataJob) syncStockOHLCV(ctx context.Context, yesterday, firstRunFrom, today time.Time) {
	from, skip := fetchFrom(yesterday, firstRunFrom, func() (time.Time, bool, error) {
		return j.store.LatestStockOHLCVDate(ctx)
	})
	if skip {
		log.Printf("[job] stock_ohlcv: up to date, skipping")
		return
	}
	raw, err := j.client.FetchAllStocksOHLCV(ctx, from, today)
	if err != nil {
		log.Printf("[job] stock_ohlcv: fetch: %v", err)
		return
	}
	clean := CleanOHLCV(raw)
	log.Printf("[job] stock_ohlcv: %d raw → %d clean (%s–%s)",
		len(raw), len(clean), from.Format(dateLayout), today.Format(dateLayout))
	if err := j.store.UpsertStockOHLCV(ctx, clean); err != nil {
		log.Printf("[job] stock_ohlcv: upsert: %v", err)
	}
}

func (j *MarketDataJob) syncForeignFlow(ctx context.Context, yesterday, firstRunFrom, today time.Time) {
	from, skip := fetchFrom(yesterday, firstRunFrom, func() (time.Time, bool, error) {
		return j.store.LatestForeignFlowDate(ctx)
	})
	if skip {
		log.Printf("[job] foreign_flow: up to date, skipping")
		return
	}
	raw, err := j.client.FetchForeignFlow(ctx, from, today)
	if err != nil {
		log.Printf("[job] foreign_flow: fetch: %v", err)
		return
	}
	clean := CleanForeignFlow(raw)
	log.Printf("[job] foreign_flow: %d raw → %d clean (%s–%s)",
		len(raw), len(clean), from.Format(dateLayout), today.Format(dateLayout))
	if err := j.store.UpsertForeignFlow(ctx, clean); err != nil {
		log.Printf("[job] foreign_flow: upsert: %v", err)
	}
}

func (j *MarketDataJob) syncIndexOHLCV(ctx context.Context, yesterday, firstRunFrom, today time.Time) {
	from, skip := fetchFrom(yesterday, firstRunFrom, func() (time.Time, bool, error) {
		return j.store.LatestIndexOHLCVDate(ctx, vnIndexSymbol)
	})
	if skip {
		log.Printf("[job] index_ohlcv: up to date, skipping")
		return
	}
	raw, err := j.client.FetchVNIndexOHLCV(ctx, from, today)
	if err != nil {
		log.Printf("[job] index_ohlcv: fetch: %v", err)
		return
	}
	clean := CleanOHLCV(raw)
	log.Printf("[job] index_ohlcv: %d raw → %d clean (%s–%s)",
		len(raw), len(clean), from.Format(dateLayout), today.Format(dateLayout))
	if err := j.store.UpsertIndexOHLCV(ctx, clean); err != nil {
		log.Printf("[job] index_ohlcv: upsert: %v", err)
	}
}

func (j *MarketDataJob) runMetricsPipeline(ctx context.Context) {
	stockHistory, err := j.store.LoadRecentStockOHLCV(ctx, stockHistoryDays)
	if err != nil {
		log.Printf("[job] metrics: load stock history: %v", err)
		return
	}

	latestRegime, hasRegime, err := j.store.LoadLatestMarketRegime(ctx)
	if err != nil {
		log.Printf("[job] metrics: load latest regime: %v — defaulting to Choppy", err)
	}
	regimeLabel := output.RegimeChoppy
	if hasRegime {
		regimeLabel = latestRegime.Regime
	}

	foreignNetBuy, err := j.store.LoadLatestForeignNetBuy(ctx)
	if err != nil {
		log.Printf("[job] metrics: load foreign net buy: %v — defaulting to false", err)
		foreignNetBuy = map[string]bool{}
	}

	metrics := make([]output.StockMetrics, 0, len(stockHistory))
	for symbol, candles := range stockHistory {
		if m, ok := calculator.ComputeStockMetrics(symbol, candles); ok {
			m.ShouldMonitorToday = calculator.ShouldMonitorToday(m)
			m.Regime = regimeLabel
			m.ForeignNetBuy = foreignNetBuy[symbol]
			m.FinalScore, m.PositionSizeFlag = calculator.ComputeFinalScore(m, j.signal)
			metrics = append(metrics, m)
		}
	}

	if err := j.store.UpsertStockMetrics(ctx, metrics); err != nil {
		log.Printf("[job] metrics: upsert stock_metrics: %v", err)
		return
	}
	log.Printf("[job] metrics: upserted %d stock_metrics records", len(metrics))

	vnHistory, err := j.store.LoadRecentIndexOHLCV(ctx, vnIndexSymbol, indexHistoryDays)
	if err != nil {
		log.Printf("[job] metrics: load vnindex history: %v", err)
		return
	}

	regime := calculator.ComputeRegime(vnHistory)
	if err := j.store.UpsertMarketRegime(ctx, regime); err != nil {
		log.Printf("[job] metrics: upsert market_regime: %v", err)
		return
	}
	log.Printf("[job] metrics: market_regime = %s on %s", regime.Regime, regime.TradingDate)
}

func fetchFrom(
	yesterday, firstRunFrom time.Time,
	latestFn func() (time.Time, bool, error),
) (from time.Time, skip bool) {
	latest, ok, err := latestFn()
	if err != nil {
		log.Printf("[job] warn: latest date query failed: %v — using default lookback", err)
		return firstRunFrom, false
	}
	if !ok {
		return firstRunFrom, false
	}
	latest = truncateDay(latest)
	if !latest.Before(yesterday) {
		return time.Time{}, true
	}
	return latest.AddDate(0, 0, 1), false
}

func truncateDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
