package job

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/order-book-imbalance/internal/calculator"
	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

type SwingSignalJob struct {
	store    output.MarketStore
	notifier output.Notifier
	ai       output.AI
	signal   config.SignalConfig
	swing    config.SwingConfig
	location *time.Location
	mu       sync.Mutex
}

func NewSwingSignalJob(
	store output.MarketStore,
	notifier output.Notifier,
	ai output.AI,
	signal config.SignalConfig,
	swing config.SwingConfig,
	location *time.Location,
) *SwingSignalJob {
	return &SwingSignalJob{
		store:    store,
		notifier: notifier,
		ai:       ai,
		signal:   signal,
		swing:    swing,
		location: location,
	}
}

func (j *SwingSignalJob) Run() {
	if !j.mu.TryLock() {
		log.Printf("[swing] already running, skipping")
		return
	}
	defer j.mu.Unlock()

	ctx := context.Background()
	today := truncateDay(time.Now().In(j.location))

	sent, err := j.store.IsSwingSignalSent(ctx, today)
	if err != nil {
		log.Printf("[swing] check sent status: %v", err)
	} else if sent {
		log.Printf("[swing] already sent today, skipping")
		return
	}

	watchlist, err := j.store.LoadWatchlist(ctx)
	if err != nil {
		log.Printf("[swing] load watchlist: %v", err)
		return
	}

	var candidates []output.WatchlistEntry
	for _, e := range watchlist {
		if e.PositionSizeFlag != "Skip" {
			candidates = append(candidates, e)
		}
	}

	sort.Slice(candidates, func(i, k int) bool {
		return candidates[i].FinalScore > candidates[k].FinalScore
	})

	if len(candidates) > j.swing.MaxWatchlistSize {
		candidates = candidates[:j.swing.MaxWatchlistSize]
	}

	if len(candidates) == 0 {
		log.Printf("[swing] watchlist empty — no signals to broadcast")
		_ = j.store.MarkSwingSignalSent(ctx, today)
		return
	}

	records := make([]output.SwingSignalRecord, 0, len(candidates))
	for _, e := range candidates {
		rec := output.SwingSignalRecord{
			Symbol:             e.Symbol,
			SignalDate:         today,
			Regime:             e.Regime,
			FinalScore:         e.FinalScore,
			PositionSizeFlag:   e.PositionSizeFlag,
			CandlePattern:      e.CandlePattern,
			VPR:                e.VPR,
			VolumeTrend:        e.VolumeTrend,
			VolumeRatio:        e.VolumeRatio,
			MomentumScore:      e.MomentumScore,
			ResistanceDistance: e.ResistanceDistance,
			Above20MA:          e.Above20MA,
			EntryPrice:         e.Close,
		}

		tp := e.ResistanceDistance
		if tp == 0 {
			tp = 0.05
		}
		rec.TPPrice = e.Close * (1 + tp)
		rec.SLPrice = e.Close * 0.97

		records = append(records, rec)
	}

	if err := j.store.LogSwingSignals(ctx, records); err != nil {
		log.Printf("[swing] log swing signals: %v", err)
	}

	regime, _, err := j.store.LoadLatestMarketRegime(ctx)
	if err != nil {
		log.Printf("[swing] load market regime: %v", err)
	}
	scoreThreshold := calculator.RegimeScoreThreshold(regime.Regime, j.signal)

	broadcastRec := output.SwingBroadcastRecord{
		SignalDate:     today,
		Regime:         string(regime.Regime),
		ScoreThreshold: scoreThreshold,
		TotalWatchlist: len(watchlist),
		Stocks:         records,
	}

	var text string
	if j.ai != nil {
		aiCtx, cancel := context.WithTimeout(ctx, j.swing.AITimeout)
		text, err = j.ai.SwingBroadcast(aiCtx, broadcastRec)
		cancel()
		if err != nil {
			log.Printf("[swing] AI broadcast: %v — using fallback", err)
			text = buildSwingFallback(broadcastRec)
		}
	} else {
		text = buildSwingFallback(broadcastRec)
	}

	if j.notifier != nil && text != "" {
		if err := j.notifier.Notify(ctx, text); err != nil {
			log.Printf("[swing] notify: %v", err)
			return
		}
	}

	if err := j.store.MarkSwingSignalSent(ctx, today); err != nil {
		log.Printf("[swing] mark sent: %v", err)
	}

	scores := make([]int, len(records))
	syms := make([]string, len(records))
	for i, r := range records {
		syms[i] = r.Symbol
		scores[i] = r.FinalScore
	}
	minScore, maxScore := scores[len(scores)-1], scores[0]
	log.Printf("[swing] broadcast sent: %d stocks [%s], score %d–%d, regime=%s",
		len(records), strings.Join(syms, ","), minScore, maxScore, regime.Regime)
}

func buildSwingFallback(rec output.SwingBroadcastRecord) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[Swing Watchlist - %s]\n", rec.SignalDate.Format("2006-01-02"))
	fmt.Fprintf(&sb, "Regime: %s | Threshold: %d\n", rec.Regime, rec.ScoreThreshold)
	for i, s := range rec.Stocks {
		fmt.Fprintf(&sb, "%d. %s — Score: %d (%s) | TP: %.2f | SL: %.2f\n",
			i+1, s.Symbol, s.FinalScore, s.PositionSizeFlag, s.TPPrice, s.SLPrice)
	}
	return sb.String()
}
