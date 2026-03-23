package job

import (
	"fmt"
	"log"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
)

const (
	minTradingDays = 5
	ssiDateLayout  = "02/01/2006"
)

func CleanOHLCV(records []input.OHLCV) []input.OHLCV {
	var valid []input.OHLCV
	for _, r := range records {
		if r.Symbol == "" {
			log.Printf("[cleaner] ohlcv skip: empty symbol")
			continue
		}
		if _, err := parseSSIDate(r.TradingDate); err != nil {
			log.Printf("[cleaner] ohlcv skip %s: unparseable date %q", r.Symbol, r.TradingDate)
			continue
		}
		if r.Close <= 0 {
			log.Printf("[cleaner] ohlcv skip %s %s: zero/negative close (%.2f)", r.Symbol, r.TradingDate, r.Close)
			continue
		}
		if r.Open <= 0 || r.High <= 0 || r.Low <= 0 {
			log.Printf("[cleaner] ohlcv skip %s %s: incomplete OHLC (O=%.2f H=%.2f L=%.2f)",
				r.Symbol, r.TradingDate, r.Open, r.High, r.Low)
			continue
		}
		if r.Volume <= 0 {
			log.Printf("[cleaner] ohlcv skip %s %s: zero volume", r.Symbol, r.TradingDate)
			continue
		}
		valid = append(valid, r)
	}

	allDates := make(map[string]struct{})
	tradingDays := make(map[string]map[string]struct{}, len(valid))
	for _, r := range valid {
		allDates[r.TradingDate] = struct{}{}
		if tradingDays[r.Symbol] == nil {
			tradingDays[r.Symbol] = make(map[string]struct{})
		}
		tradingDays[r.Symbol][r.TradingDate] = struct{}{}
	}

	threshold := min(minTradingDays, len(allDates))

	excludeReasons := make(map[string]string)
	for sym, days := range tradingDays {
		if len(days) < threshold {
			excludeReasons[sym] = fmt.Sprintf(
				"%d trading day(s) in window (min %d) — possibly newly listed, suspended, or delisted",
				len(days), threshold,
			)
		}
	}
	for sym, reason := range excludeReasons {
		log.Printf("[cleaner] ohlcv exclude %s: %s", sym, reason)
	}

	out := make([]input.OHLCV, 0, len(valid))
	for _, r := range valid {
		if _, skip := excludeReasons[r.Symbol]; !skip {
			out = append(out, r)
		}
	}
	return out
}

func CleanForeignFlow(records []input.ForeignFlow) []input.ForeignFlow {
	out := make([]input.ForeignFlow, 0, len(records))
	for _, r := range records {
		if r.Symbol == "" {
			log.Printf("[cleaner] foreign_flow skip: empty symbol")
			continue
		}
		if _, err := parseSSIDate(r.TradingDate); err != nil {
			log.Printf("[cleaner] foreign_flow skip %s: unparseable date %q", r.Symbol, r.TradingDate)
			continue
		}
		out = append(out, r)
	}
	return out
}

func parseSSIDate(s string) (time.Time, error) {
	return time.Parse(ssiDateLayout, s)
}
