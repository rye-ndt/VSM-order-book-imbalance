package job

import (
	"context"
	"log"
	"strings"
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
	client   input.StockDataClient
	store    output.MarketStore
	signal   config.SignalConfig
	location *time.Location
	mu       sync.Mutex
}

func NewMarketDataJob(
	client input.StockDataClient,
	store output.MarketStore,
	signal config.SignalConfig,
	location *time.Location,
) *MarketDataJob {
	return &MarketDataJob{client: client, store: store, signal: signal, location: location}
}

func (j *MarketDataJob) Run() {
	if !j.mu.TryLock() {
		log.Printf("[job] market data: already running, skipping")
		return
	}
	defer j.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	now := time.Now().In(j.location)
	today := truncateDay(now)
	yesterday := today.AddDate(0, 0, -1)
	firstRunFrom := today.Add(-defaultLookback)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); j.syncStockOHLCV(ctx, yesterday, firstRunFrom, today) }()
	go func() { defer wg.Done(); j.syncForeignFlow(ctx, yesterday, firstRunFrom, today) }()
	go func() { defer wg.Done(); j.syncIndexOHLCV(ctx, yesterday, firstRunFrom, today) }()
	wg.Wait()

	j.runMetricsPipeline(ctx)

	if err := j.store.MarkMarketDataCrawled(ctx, today); err != nil {
		log.Printf("[job] mark market data crawled: %v", err)
	}

	n, err := j.store.BackfillSignalOutcomes(ctx)
	if err != nil {
		log.Printf("[job] backfill signal outcomes: %v", err)
	} else if n > 0 {
		log.Printf("[job] backfilled %d signal outcome fields", n)
	}

	n2, err := j.store.BackfillSwingOutcomes(ctx)
	if err != nil {
		log.Printf("[job] backfill swing outcomes: %v", err)
	} else if n2 > 0 {
		log.Printf("[job] backfilled %d swing outcome fields", n2)
	}

	n3, err := j.store.BackfillATODailyOutcomes(ctx)
	if err != nil {
		log.Printf("[job] backfill ato daily outcomes: %v", err)
	} else if n3 > 0 {
		log.Printf("[job] backfilled %d ato daily outcome fields", n3)
	}

	j.runAudit(ctx, today)
}

func (j *MarketDataJob) runAudit(ctx context.Context, today time.Time) {
	warn := func(format string, args ...any) {
		log.Printf("[audit] WARN "+format, args...)
	}
	info := func(format string, args ...any) {
		log.Printf("[audit] "+format, args...)
	}

	const stalenessThreshold = 5 * 24 * time.Hour

	stockDate, stockOk, err := j.store.LatestStockOHLCVDate(ctx)
	if err != nil {
		warn("latest stock_ohlcv date: %v", err)
	} else if !stockOk {
		warn("stock_ohlcv is empty")
	} else {
		age := today.Sub(truncateDay(stockDate))
		info("stock_ohlcv latest=%s (%.0f days ago)", stockDate.Format(dateLayout), age.Hours()/24)
		if age > stalenessThreshold {
			warn("stock_ohlcv is stale — latest=%s, today=%s", stockDate.Format(dateLayout), today.Format(dateLayout))
		}
	}

	indexDate, indexOk, err := j.store.LatestIndexOHLCVDate(ctx, vnIndexSymbol)
	if err != nil {
		warn("latest index_ohlcv date: %v", err)
	} else if !indexOk {
		warn("index_ohlcv is empty")
	} else {
		age := today.Sub(truncateDay(indexDate))
		info("index_ohlcv latest=%s (%.0f days ago)", indexDate.Format(dateLayout), age.Hours()/24)
		if age > stalenessThreshold {
			warn("index_ohlcv is stale — latest=%s", indexDate.Format(dateLayout))
		}
		if stockOk && !truncateDay(indexDate).Equal(truncateDay(stockDate)) {
			warn("index_ohlcv date (%s) != stock_ohlcv date (%s) — regime will be computed from older index data",
				indexDate.Format(dateLayout), stockDate.Format(dateLayout))
		}
	}

	regime, hasRegime, err := j.store.LoadLatestMarketRegime(ctx)
	if err != nil {
		warn("load market regime: %v", err)
	} else if !hasRegime {
		warn("market_regime is empty")
	} else {
		info("market_regime=%s on %s", regime.Regime, regime.TradingDate)
		if indexOk {
			regimeDate, _ := time.Parse("02/01/2006", regime.TradingDate)
			if !truncateDay(regimeDate).Equal(truncateDay(indexDate)) {
				warn("market_regime date (%s) != index_ohlcv date (%s) — regime is stale",
					regime.TradingDate, indexDate.Format(dateLayout))
			}
		}
	}

	watchlist, err := j.store.LoadWatchlist(ctx)
	if err != nil {
		warn("load watchlist: %v", err)
	} else {
		full, half := 0, 0
		for _, e := range watchlist {
			switch e.PositionSizeFlag {
			case "Full":
				full++
			case "Half":
				half++
			}
		}
		info("watchlist: %d total (%d Full, %d Half)", len(watchlist), full, half)
		if len(watchlist) == 0 {
			warn("watchlist is empty — no stocks will be monitored tomorrow")
		} else if len(watchlist) < 5 {
			warn("watchlist has only %d stocks — unusually small", len(watchlist))
		}
	}
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

	vnHistory, err := j.store.LoadRecentIndexOHLCV(ctx, vnIndexSymbol, indexHistoryDays)
	if err != nil {
		log.Printf("[job] metrics: load vnindex history: %v", err)
		return
	}
	if len(vnHistory) == 0 {
		log.Printf("[job] metrics: vnindex history empty — regime not computed (no index data in DB)")
		return
	}
	log.Printf("[job] metrics: vnindex history loaded: %d candles (%s – %s)",
		len(vnHistory), vnHistory[0].TradingDate, vnHistory[len(vnHistory)-1].TradingDate)

	regime := calculator.ComputeRegime(vnHistory)
	if err := j.store.UpsertMarketRegime(ctx, regime); err != nil {
		log.Printf("[job] metrics: upsert market_regime: %v", err)
		return
	}
	log.Printf("[job] metrics: market_regime = %s on %s", regime.Regime, regime.TradingDate)

	foreignNetBuy, err := j.store.LoadLatestForeignNetBuy(ctx)
	if err != nil {
		log.Printf("[job] metrics: load foreign net buy: %v — defaulting to false", err)
		foreignNetBuy = map[string]bool{}
	}

	metrics := make([]output.StockMetrics, 0, len(stockHistory))
	for symbol, candles := range stockHistory {
		if !isEquity(symbol) {
			continue
		}
		if m, ok := calculator.ComputeStockMetrics(symbol, candles); ok {
			m.ShouldMonitorToday = calculator.ShouldMonitorToday(m)
			m.Regime = regime.Regime
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

func isEquity(symbol string) bool {
	if strings.HasPrefix(symbol, "FUE") {
		return false
	}
	n := len(symbol)
	if n >= 7 && symbol[0] == 'C' {
		for _, ch := range symbol[n-4:] {
			if ch < '0' || ch > '9' {
				return true
			}
		}
		return false
	}
	return true
}
