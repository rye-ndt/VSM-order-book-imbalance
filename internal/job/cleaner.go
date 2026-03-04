package job

import (
	"fmt"
	"log"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
)

// minTradingDays is the minimum number of distinct trading days a symbol must
// have within the fetched window to be considered sufficiently listed.
// Symbols below this threshold are likely newly listed, recently suspended, or
// about to be delisted, and are excluded from storage.
const minTradingDays = 5

// CleanOHLCV removes invalid rows and excludes symbols without enough history.
//
// Row-level rules (any failure drops the row):
//   - Symbol must not be empty
//   - TradingDate must parse as DD/MM/YYYY
//   - Close must be > 0  (zero close indicates missing/bad data)
//   - Open, High, Low must all be > 0
//   - Volume must be > 0  (zero volume = no trades; not useful for analysis)
//
// Symbol-level rules (applied after row cleaning):
//   - A symbol with fewer than minTradingDays valid rows in the window is
//     excluded entirely (newly listed, prolonged suspension, etc.)
//
// Every exclusion is written to the log with a reason.
func CleanOHLCV(records []input.OHLCV) []input.OHLCV {
	// --- pass 1: row-level validation ---
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
			log.Printf("[cleaner] ohlcv skip %s %s: incomplete OHLC (O=%.2f H=%.2f L=%.2f)", r.Symbol, r.TradingDate, r.Open, r.High, r.Low)
			continue
		}
		if r.Volume <= 0 {
			log.Printf("[cleaner] ohlcv skip %s %s: zero volume", r.Symbol, r.TradingDate)
			continue
		}
		valid = append(valid, r)
	}

	// --- pass 2: symbol-level — count distinct trading days per symbol ---
	tradingDays := make(map[string]map[string]struct{}, len(valid))
	for _, r := range valid {
		if tradingDays[r.Symbol] == nil {
			tradingDays[r.Symbol] = make(map[string]struct{})
		}
		tradingDays[r.Symbol][r.TradingDate] = struct{}{}
	}

	excluded := make(map[string]string) // symbol → reason
	for sym, days := range tradingDays {
		if len(days) < minTradingDays {
			excluded[sym] = fmt.Sprintf(
				"%d trading day(s) in window (min %d) — possibly newly listed, suspended, or delisted",
				len(days), minTradingDays,
			)
		}
	}
	for sym, reason := range excluded {
		log.Printf("[cleaner] ohlcv exclude %s: %s", sym, reason)
	}

	out := make([]input.OHLCV, 0, len(valid))
	for _, r := range valid {
		if _, skip := excluded[r.Symbol]; !skip {
			out = append(out, r)
		}
	}
	return out
}

// CleanForeignFlow removes rows with missing symbols or unparseable dates.
// Zero buy/sell activity is valid (no foreign participation that day) and is
// retained.  Symbols are NOT subject to the minimum-days rule here because a
// single foreign-flow snapshot per day is meaningful on its own.
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
	return time.Parse("02/01/2006", s)
}
