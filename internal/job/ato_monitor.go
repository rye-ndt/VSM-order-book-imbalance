package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/example/order-book-imbalance/internal/calculator"
	"github.com/example/order-book-imbalance/internal/interface/input"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

const (
	atoPollInterval = 2 * time.Second
	atoLocation     = "Asia/Ho_Chi_Minh"

	atoEndHour   = 9
	atoEndMinute = 15

	atoDropHour   = 9
	atoDropMinute = 7

	atoSignalStopHour   = 9
	atoSignalStopMinute = 14

	// maxGapFromRef is the maximum allowed gap between IndicatedPrice and
	// RefPrice (prior-day close) for a signal to fire.
	maxGapFromRef = 0.05

	// atoTPRatio is the take-profit target expressed as a multiplier of the
	// ATO indicated price (e.g. 1.03 = 3% above entry).
	atoTPRatio = 1.03
)

type ATOMonitorJob struct {
	store    output.MarketStore
	obClient input.OrderBookClient
	notifier output.Notifier
}

func NewATOMonitorJob(store output.MarketStore, obClient input.OrderBookClient, notifier output.Notifier) *ATOMonitorJob {
	return &ATOMonitorJob{store: store, obClient: obClient, notifier: notifier}
}

func (j *ATOMonitorJob) Run() {
	loc, err := time.LoadLocation(atoLocation)
	if err != nil {
		log.Printf("[ato] load timezone: %v", err)
		return
	}

	now := time.Now().In(loc)
	stopAt       := time.Date(now.Year(), now.Month(), now.Day(), atoEndHour, atoEndMinute, 0, 0, loc)
	dropAt       := time.Date(now.Year(), now.Month(), now.Day(), atoDropHour, atoDropMinute, 0, 0, loc)
	signalStopAt := time.Date(now.Year(), now.Month(), now.Day(), atoSignalStopHour, atoSignalStopMinute, 0, 0, loc)

	if !time.Now().Before(stopAt) {
		log.Printf("[ato] already past 09:15 ICT, skipping")
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), stopAt)
	defer cancel()

	regime, hasRegime, err := j.store.LoadLatestMarketRegime(ctx)
	if err != nil {
		log.Printf("[ato] load market regime: %v — defaulting to Choppy", err)
	}
	regimeLabel := output.RegimeChoppy
	if hasRegime {
		regimeLabel = regime.Regime
	}
	scoreThreshold := calculator.RegimeScoreThreshold(regimeLabel)
	log.Printf("[ato] market regime: %s  score threshold: %d", regimeLabel, scoreThreshold)

	watchlist, err := j.store.LoadWatchlist(ctx)
	if err != nil {
		log.Printf("[ato] load watchlist: %v", err)
		return
	}
	if len(watchlist) == 0 {
		log.Printf("[ato] watchlist empty, exiting")
		return
	}

	entries := make(map[string]output.WatchlistEntry, len(watchlist))
	active := make(map[string]bool, len(watchlist))
	for _, e := range watchlist {
		entries[e.Symbol] = e
		active[e.Symbol] = true
	}
	log.Printf("[ato] monitoring %d stocks until 09:15 ICT", len(active))

	symbols := make([]string, 0, len(watchlist))
	for _, e := range watchlist {
		symbols = append(symbols, e.Symbol)
	}
	if err := j.obClient.Subscribe(ctx, symbols); err != nil {
		log.Printf("[ato] subscribe: %v", err)
		return
	}

	windows := make(map[string]*calculator.StabilityWindow, len(active))
	for sym := range active {
		windows[sym] = &calculator.StabilityWindow{Symbol: sym}
	}

	fired := make(map[string]bool, len(active))

	dropped        := false
	signalsStopped := false
	ticker := time.NewTicker(atoPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logSessionSummary(fired, active)
			return
		case t := <-ticker.C:
			if !dropped && t.After(dropAt) {
				dropped = true
				dropNoPrice(active, windows)
			}
			if len(active) == 0 {
				log.Printf("[ato] all symbols dropped, exiting early")
				return
			}
			if !signalsStopped && t.After(signalStopAt) {
				signalsStopped = true
				resetStabilityWindows(windows)
				log.Printf("[ato] 09:14 ICT — signal window closed, stability windows reset")
			}
			pollOnce(ctx, j.store, j.obClient, j.notifier, active, windows, entries, fired, scoreThreshold, signalsStopped)
		}
	}
}

func resetStabilityWindows(windows map[string]*calculator.StabilityWindow) {
	for sym := range windows {
		windows[sym].Snapshots = nil
		windows[sym].StableCount = 0
	}
}

func dropNoPrice(active map[string]bool, windows map[string]*calculator.StabilityWindow) {
	for sym := range active {
		snaps := windows[sym].Snapshots
		if len(snaps) == 0 || snaps[len(snaps)-1].IndicatedPrice == 0 {
			log.Printf("[ato] %s: no indicated price by 09:07 ICT, dropping", sym)
			delete(active, sym)
		}
	}
}

func pollOnce(
	ctx context.Context,
	store output.MarketStore,
	obClient input.OrderBookClient,
	notifier output.Notifier,
	active map[string]bool,
	windows map[string]*calculator.StabilityWindow,
	entries map[string]output.WatchlistEntry,
	fired map[string]bool,
	scoreThreshold int,
	signalsStopped bool,
) {
	for sym := range active {
		snap, err := obClient.FetchOrderBook(sym)
		if errors.Is(err, input.ErrNoSnapshot) {
			continue
		}
		if err != nil {
			log.Printf("[ato] %s: fetch: %v", sym, err)
			continue
		}
		if snap.IndicatedPrice == 0 {
			continue
		}

		analysis, isStable := calculator.ProcessSnapshot(snap, windows[sym])
		log.Printf("[ato] %s  ratio=%.2fx  bid_lvls=%d  spoof=%.0f%%  ask_wall=%v  stable=%d",
			sym,
			analysis.ImbalanceRatio,
			analysis.BidLevelCount,
			analysis.LargestBidFraction*100,
			analysis.HasAskWall,
			windows[sym].StableCount,
		)

		if !isStable || fired[sym] || signalsStopped {
			continue
		}

		entry := entries[sym]
		reason, ok := checkSignalGate(snap, entry.FinalScore, scoreThreshold)
		if !ok {
			log.Printf("[ato] %s: signal gate blocked — %s", sym, reason)
			continue
		}

		fired[sym] = true
		tpPrice := snap.IndicatedPrice * atoTPRatio
		msg := fmt.Sprintf(
			"ATO SIGNAL: %s\nRatio: %.2fx  Score: %d  Regime threshold: %d\nIndicated: %.0f  Ceiling: %.0f  Ref: %.0f\nTP: %.0f\nStable snapshots: %d",
			sym, analysis.ImbalanceRatio, entry.FinalScore, scoreThreshold,
			snap.IndicatedPrice, snap.CeilingPrice, snap.RefPrice,
			tpPrice,
			windows[sym].StableCount,
		)
		log.Printf("[ato] SIGNAL %s", msg)
		if notifier != nil {
			if err := notifier.Notify(ctx, msg); err != nil {
				log.Printf("[ato] notify: %v", err)
			}
		}

		var openGap float64
		if snap.RefPrice > 0 {
			openGap = (snap.IndicatedPrice - snap.RefPrice) / snap.RefPrice
		}
		rec := output.SignalRecord{
			Symbol:             sym,
			FinalScore:         entry.FinalScore,
			IndicatedPrice:     snap.IndicatedPrice,
			TPPrice:            tpPrice,
			FiredAt:            time.Now(),
			OpenGap:            openGap,
			ImbalanceRatio:     analysis.ImbalanceRatio,
			SnapshotCount:      windows[sym].StableCount,
			SnapshotFiredAt:    analysis.CapturedAt,
			Regime:             entry.Regime,
			CandlePattern:      entry.CandlePattern,
			VPR:                entry.VPR,
			VolumeTrend:        entry.VolumeTrend,
			VolumeRatio:        entry.VolumeRatio,
			MomentumScore:      entry.MomentumScore,
			ResistanceDistance: entry.ResistanceDistance,
			Above20MA:          entry.Above20MA,
			PositionSizeFlag:   entry.PositionSizeFlag,
		}
		if err := store.LogSignal(ctx, rec); err != nil {
			log.Printf("[ato] %s: log signal: %v", sym, err)
		}
	}
}

// checkSignalGate validates all conditions that must hold at signal fire time.
// Returns a reason string and false if any condition fails.
func checkSignalGate(snap input.OrderBookSnapshot, score, threshold int) (reason string, ok bool) {
	if score < threshold {
		return fmt.Sprintf("score %d below regime threshold %d", score, threshold), false
	}
	if snap.CeilingPrice > 0 && snap.IndicatedPrice >= snap.CeilingPrice {
		return fmt.Sprintf("indicated %.0f >= ceiling %.0f (at tran price)", snap.IndicatedPrice, snap.CeilingPrice), false
	}
	if snap.RefPrice > 0 {
		gap := (snap.IndicatedPrice - snap.RefPrice) / snap.RefPrice
		if gap >= maxGapFromRef {
			return fmt.Sprintf("gap from ref %.1f%% >= 5%%", gap*100), false
		}
	}
	return "", true
}

func logSessionSummary(fired, active map[string]bool) {
	total := len(active)
	signalCount := 0
	for _, f := range fired {
		if f {
			signalCount++
		}
	}
	log.Printf("[ato] session ended — monitored: %d  signals fired: %d  no signal: %d",
		total, signalCount, total-signalCount)
}
