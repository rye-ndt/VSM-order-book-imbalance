package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/example/order-book-imbalance/internal/calculator"
	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

type ATOMonitorJob struct {
	store        output.MarketStore
	obClient     input.OrderBookClient
	notifier     output.Notifier
	socialPoster output.SocialPoster
	ai           output.AI
	signal       config.SignalConfig
	ato          config.ATOConfig
}

type atoCandidate struct {
	sym         string
	snap        input.OrderBookSnapshot
	analysis    calculator.SnapshotAnalysis
	entry       output.WatchlistEntry
	stableCount int
}

func NewATOMonitorJob(
	store output.MarketStore,
	obClient input.OrderBookClient,
	notifier output.Notifier,
	socialPoster output.SocialPoster,
	ai output.AI,
	signal config.SignalConfig,
	ato config.ATOConfig,
) *ATOMonitorJob {
	return &ATOMonitorJob{
		store:        store,
		obClient:     obClient,
		notifier:     notifier,
		socialPoster: socialPoster,
		ai:           ai,
		signal:       signal,
		ato:          ato,
	}
}

func (j *ATOMonitorJob) Run() {
	loc, err := time.LoadLocation(j.ato.Timezone)
	if err != nil {
		log.Printf("[ato] load timezone: %v", err)
		return
	}

	now := time.Now().In(loc)
	localTime := func(h, m int) time.Time {
		return time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, loc)
	}
	stopAt := localTime(j.ato.EndHour, j.ato.EndMinute)
	dropAt := localTime(j.ato.DropHour, j.ato.DropMinute)
	signalStopAt := localTime(j.ato.SignalStopHour, j.ato.SignalStopMinute)

	if !time.Now().Before(stopAt) {
		log.Printf("[ato] already past %02d:%02d ICT, skipping", j.ato.EndHour, j.ato.EndMinute)
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
	scoreThreshold := calculator.RegimeScoreThreshold(regimeLabel, j.signal)
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
	log.Printf("[ato] monitoring %d stocks until %02d:%02d ICT", len(active), j.ato.EndHour, j.ato.EndMinute)

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
	dropped := false
	signalsStopped := false
	ticker := time.NewTicker(j.ato.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logSessionSummary(fired, active)
			return
		case t := <-ticker.C:
			if !dropped && t.After(dropAt) {
				dropped = true
				dropNoPrice(active, windows, j.ato.DropHour, j.ato.DropMinute)
			}
			if len(active) == 0 {
				log.Printf("[ato] all symbols dropped, exiting early")
				return
			}
			if !signalsStopped && t.After(signalStopAt) {
				signalsStopped = true
				resetStabilityWindows(windows)
				log.Printf("[ato] %02d:%02d ICT — signal window closed, stability windows reset",
					j.ato.SignalStopHour, j.ato.SignalStopMinute)
			}
			for _, c := range j.poll(active, windows, entries, fired, scoreThreshold, signalsStopped) {
				fired[c.sym] = true
				j.fireSignal(ctx, c, scoreThreshold)
			}
		}
	}
}

func (j *ATOMonitorJob) poll(
	active map[string]bool,
	windows map[string]*calculator.StabilityWindow,
	entries map[string]output.WatchlistEntry,
	fired map[string]bool,
	scoreThreshold int,
	signalsStopped bool,
) []atoCandidate {
	var candidates []atoCandidate
	for sym := range active {
		snap, err := j.obClient.FetchOrderBook(sym)
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

		analysis, isStable := calculator.ProcessSnapshot(snap, windows[sym], j.signal)
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

		reason, ok := checkSignalGate(snap, entries[sym].FinalScore, scoreThreshold, j.signal)
		if !ok {
			log.Printf("[ato] %s: signal gate blocked — %s", sym, reason)
			continue
		}

		candidates = append(candidates, atoCandidate{
			sym:         sym,
			snap:        snap,
			analysis:    analysis,
			entry:       entries[sym],
			stableCount: windows[sym].StableCount,
		})
	}

	return candidates
}

func (j *ATOMonitorJob) fireSignal(ctx context.Context, c atoCandidate, scoreThreshold int) {
	tpPrice := c.snap.IndicatedPrice * j.signal.TPRatio
	msg := fmt.Sprintf(
		"ATO SIGNAL: %s\nRatio: %.2fx  Score: %d  Regime threshold: %d\n"+
			"Indicated: %.0f  Ceiling: %.0f  Ref: %.0f\nTP: %.0f\nStable snapshots: %d",
		c.sym, c.analysis.ImbalanceRatio, c.entry.FinalScore, scoreThreshold,
		c.snap.IndicatedPrice, c.snap.CeilingPrice, c.snap.RefPrice,
		tpPrice, c.stableCount,
	)
	log.Printf("[ato] SIGNAL %s", msg)
	if j.notifier != nil {
		if err := j.notifier.Notify(ctx, msg); err != nil {
			log.Printf("[ato] notify: %v", err)
		}
	}

	var openGap float64
	if c.snap.RefPrice > 0 {
		openGap = (c.snap.IndicatedPrice - c.snap.RefPrice) / c.snap.RefPrice
	}
	rec := output.SignalRecord{
		Symbol:             c.sym,
		FinalScore:         c.entry.FinalScore,
		IndicatedPrice:     c.snap.IndicatedPrice,
		TPPrice:            tpPrice,
		FiredAt:            time.Now(),
		OpenGap:            openGap,
		ImbalanceRatio:     c.analysis.ImbalanceRatio,
		SnapshotCount:      c.stableCount,
		SnapshotFiredAt:    c.analysis.CapturedAt,
		Regime:             c.entry.Regime,
		CandlePattern:      c.entry.CandlePattern,
		VPR:                c.entry.VPR,
		VolumeTrend:        c.entry.VolumeTrend,
		VolumeRatio:        c.entry.VolumeRatio,
		MomentumScore:      c.entry.MomentumScore,
		ResistanceDistance: c.entry.ResistanceDistance,
		Above20MA:          c.entry.Above20MA,
		PositionSizeFlag:   c.entry.PositionSizeFlag,
	}
	if err := j.store.LogSignal(ctx, rec); err != nil {
		log.Printf("[ato] %s: log signal: %v", c.sym, err)
	}

	if j.ai != nil {
		go j.postAISignal(rec, scoreThreshold)
	}
}

func (j *ATOMonitorJob) postAISignal(rec output.SignalRecord, threshold int) {
	ctx, cancel := context.WithTimeout(context.Background(), j.ato.AITimeout)
	defer cancel()

	interp, err := j.ai.Interpret(ctx, rec, threshold)
	if err != nil {
		log.Printf("[ato] %s: ai interpret: %v", rec.Symbol, err)
		return
	}

	if j.socialPoster != nil {
		xText, err := j.ai.XInterpret(ctx, interp)
		if err != nil {
			log.Printf("[ato] %s: ai x interpret: %v", rec.Symbol, err)
		} else if xText != "" {
			if err := j.socialPoster.Post(ctx, xText); err != nil {
				log.Printf("[ato] %s: x post: %v", rec.Symbol, err)
			}
		}
	}

	if j.notifier != nil {
		tgText, err := j.ai.TelegramInterpret(ctx, interp)
		if err != nil {
			log.Printf("[ato] %s: ai telegram interpret: %v", rec.Symbol, err)
		} else if tgText != "" {
			if err := j.notifier.Notify(ctx, tgText); err != nil {
				log.Printf("[ato] %s: ai telegram notify: %v", rec.Symbol, err)
			}
		}
	}
}

func resetStabilityWindows(windows map[string]*calculator.StabilityWindow) {
	for sym := range windows {
		windows[sym].Snapshots = nil
		windows[sym].StableCount = 0
	}
}

func dropNoPrice(active map[string]bool, windows map[string]*calculator.StabilityWindow, dropHour, dropMinute int) {
	for sym := range active {
		snaps := windows[sym].Snapshots
		if len(snaps) == 0 || snaps[len(snaps)-1].IndicatedPrice == 0 {
			log.Printf("[ato] %s: no indicated price by %02d:%02d ICT, dropping", sym, dropHour, dropMinute)
			delete(active, sym)
		}
	}
}

func checkSignalGate(
	snap input.OrderBookSnapshot, score, threshold int, signal config.SignalConfig,
) (reason string, ok bool) {
	if score < threshold {
		return fmt.Sprintf("score %d below regime threshold %d", score, threshold), false
	}
	if snap.CeilingPrice > 0 && snap.IndicatedPrice >= snap.CeilingPrice {
		return fmt.Sprintf("indicated %.0f >= ceiling %.0f (at tran price)", snap.IndicatedPrice, snap.CeilingPrice), false
	}
	if snap.RefPrice > 0 {
		gap := (snap.IndicatedPrice - snap.RefPrice) / snap.RefPrice
		if gap >= signal.MaxGapFromRef {
			return fmt.Sprintf("gap from ref %.1f%% >= %.0f%%", gap*100, signal.MaxGapFromRef*100), false
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
