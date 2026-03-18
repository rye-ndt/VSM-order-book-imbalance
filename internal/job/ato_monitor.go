package job

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
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
	mu           sync.Mutex
}

type atoCandidate struct {
	sym         string
	snap        input.OrderBookSnapshot
	analysis    calculator.SnapshotAnalysis
	entry       output.WatchlistEntry
	stableCount int
}

type liveView struct {
	snap     input.OrderBookSnapshot
	analysis calculator.SnapshotAnalysis
	score    int
	stable   int
}

type sessionLogger struct {
	store       output.MarketStore
	sessionDate time.Time
}

func (sl *sessionLogger) info(symbol, event, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[ato] %s", msg)
	sl.write("INFO", symbol, event, msg)
}

func (sl *sessionLogger) warn(symbol, event, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[ato] WARN %s", msg)
	sl.write("WARN", symbol, event, msg)
}

func (sl *sessionLogger) write(level, symbol, event, msg string) {
	if err := sl.store.AppendSessionLog(context.Background(), output.SessionLogEntry{
		SessionDate: sl.sessionDate,
		Level:       level,
		Component:   "ato",
		Symbol:      symbol,
		Event:       event,
		Message:     msg,
	}); err != nil {
		log.Printf("[ato] session log db write: %v", err)
	}
}

type sessionState struct {
	quoteCount map[string]int
	hadPrice   map[string]bool
	seenQuote  map[string]bool
	seenStable map[string]bool
	gateReason map[string]string
}

func newSessionState(n int) *sessionState {
	return &sessionState{
		quoteCount: make(map[string]int, n),
		hadPrice:   make(map[string]bool, n),
		seenQuote:  make(map[string]bool, n),
		seenStable: make(map[string]bool, n),
		gateReason: make(map[string]string, n),
	}
}

const (
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBold   = "\033[1m"
	ansiReset  = "\033[0m"
	clearTerm  = "\033[H\033[2J"
	obBarWidth  = 32
	obMaxLevels = 5
	obMaxSyms   = 8
)

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
	if !j.mu.TryLock() {
		log.Printf("[ato] already running, skipping")
		return
	}
	defer j.mu.Unlock()

	loc, err := time.LoadLocation(j.ato.Timezone)
	if err != nil {
		log.Printf("[ato] load timezone: %v", err)
		return
	}

	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	sl := &sessionLogger{store: j.store, sessionDate: today}

	localTime := func(h, m int) time.Time {
		return time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, loc)
	}
	stopAt := localTime(j.ato.EndHour, j.ato.EndMinute)
	dropAt := localTime(j.ato.DropHour, j.ato.DropMinute)
	signalStopAt := localTime(j.ato.SignalStopHour, j.ato.SignalStopMinute)

	if !time.Now().Before(stopAt) {
		sl.warn("", "session_skipped", "already past %02d:%02d ICT, skipping", j.ato.EndHour, j.ato.EndMinute)
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), stopAt)
	defer cancel()

	regime, hasRegime, err := j.store.LoadLatestMarketRegime(ctx)
	if err != nil {
		sl.warn("", "regime_load_error", "load market regime: %v — defaulting to Choppy", err)
	}
	regimeLabel := output.RegimeChoppy
	if hasRegime {
		regimeLabel = regime.Regime
	}
	scoreThreshold := calculator.RegimeScoreThreshold(regimeLabel, j.signal)
	sl.info("", "regime_loaded", "market regime: %s  score threshold: %d", regimeLabel, scoreThreshold)

	watchlist, err := j.store.LoadWatchlist(ctx)
	if err != nil {
		sl.warn("", "watchlist_load_error", "load watchlist: %v", err)
		return
	}
	if len(watchlist) == 0 {
		sl.warn("", "watchlist_empty", "watchlist empty, exiting")
		return
	}

	entries := make(map[string]output.WatchlistEntry, len(watchlist))
	active := make(map[string]bool, len(watchlist))
	for _, e := range watchlist {
		entries[e.Symbol] = e
		active[e.Symbol] = true
	}
	sl.info("", "session_start", "monitoring %d stocks until %02d:%02d ICT", len(active), j.ato.EndHour, j.ato.EndMinute)

	symbols := make([]string, 0, len(watchlist))
	for _, e := range watchlist {
		symbols = append(symbols, e.Symbol)
	}
	if err := j.obClient.Subscribe(ctx, symbols); err != nil {
		sl.warn("", "subscribe_error", "subscribe: %v", err)
		return
	}

	if err := j.store.MarkATOMonitored(context.Background(), today); err != nil {
		log.Printf("[ato] mark monitored: %v", err)
	}

	windows := make(map[string]*calculator.StabilityWindow, len(active))
	for sym := range active {
		windows[sym] = &calculator.StabilityWindow{Symbol: sym}
	}

	ss := newSessionState(len(active))
	fired := make(map[string]bool, len(active))
	dropped := false
	signalsStopped := false
	ticker := time.NewTicker(j.ato.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logSessionSummary(fired, active, ss, sl)
			return
		case t := <-ticker.C:
			if !dropped && t.After(dropAt) {
				dropped = true
				dropNoPrice(active, windows, ss, j.ato.DropHour, j.ato.DropMinute, sl)
			}
			if len(active) == 0 {
				sl.warn("", "all_dropped", "all symbols dropped, exiting early")
				return
			}
			if !signalsStopped && t.After(signalStopAt) {
				signalsStopped = true
				resetStabilityWindows(windows)
				sl.info("", "signal_window_closed", "%02d:%02d ICT — signal window closed, stability windows reset",
					j.ato.SignalStopHour, j.ato.SignalStopMinute)
			}
			views := make(map[string]liveView)
			for _, c := range j.poll(active, windows, entries, fired, scoreThreshold, signalsStopped, views, ss, sl) {
				fired[c.sym] = true
				j.fireSignal(ctx, c, scoreThreshold, sl)
			}
			renderOrderBooks(views, t.In(loc))
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
	views map[string]liveView,
	ss *sessionState,
	sl *sessionLogger,
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

		ss.quoteCount[sym]++
		if !ss.seenQuote[sym] {
			ss.seenQuote[sym] = true
			sl.info(sym, "first_quote", "%s: first Quote received  indicated=%.0f  ceil=%.0f  ref=%.0f",
				sym, snap.IndicatedPrice, snap.CeilingPrice, snap.RefPrice)
		}

		if snap.IndicatedPrice == 0 {
			continue
		}
		if !ss.hadPrice[sym] {
			ss.hadPrice[sym] = true
		}

		analysis, isStable := calculator.ProcessSnapshot(snap, windows[sym], j.signal)
		var gapPct float64
		if snap.RefPrice > 0 {
			gapPct = (snap.IndicatedPrice - snap.RefPrice) / snap.RefPrice * 100
		}
		log.Printf("[ato] %s  price=%.0f  gap=%+.1f%%  ratio=%.2fx  bid_lvls=%d  spoof=%.0f%%  ask_wall=%v  stable=%d  score=%d",
			sym,
			snap.IndicatedPrice,
			gapPct,
			analysis.ImbalanceRatio,
			analysis.BidLevelCount,
			analysis.LargestBidFraction*100,
			analysis.HasAskWall,
			windows[sym].StableCount,
			entries[sym].FinalScore,
		)
		views[sym] = liveView{
			snap:     snap,
			analysis: analysis,
			score:    entries[sym].FinalScore,
			stable:   windows[sym].StableCount,
		}

		if isStable && !ss.seenStable[sym] {
			ss.seenStable[sym] = true
			sl.info(sym, "stability_achieved", "%s: stability achieved  ratio=%.2fx  stable=%d  score=%d",
				sym, analysis.ImbalanceRatio, windows[sym].StableCount, entries[sym].FinalScore)
		}

		if !isStable || fired[sym] || signalsStopped {
			continue
		}

		reason, ok := checkSignalGate(snap, entries[sym].FinalScore, scoreThreshold, j.signal)
		if !ok {
			sl.info(sym, "gate_blocked", "%s: signal gate blocked — %s", sym, reason)
			ss.gateReason[sym] = reason
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

func (j *ATOMonitorJob) fireSignal(ctx context.Context, c atoCandidate, scoreThreshold int, sl *sessionLogger) {
	tpPrice := c.snap.IndicatedPrice * j.signal.TPRatio
	msg := fmt.Sprintf(
		"ATO SIGNAL: %s\nRatio: %.2fx  Score: %d  Regime threshold: %d\n"+
			"Indicated: %.0f  Ceiling: %.0f  Ref: %.0f\nTP: %.0f\nStable snapshots: %d",
		c.sym, c.analysis.ImbalanceRatio, c.entry.FinalScore, scoreThreshold,
		c.snap.IndicatedPrice, c.snap.CeilingPrice, c.snap.RefPrice,
		tpPrice, c.stableCount,
	)
	sl.info(c.sym, "signal_fired", "SIGNAL %s", msg)
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

func dropNoPrice(active map[string]bool, windows map[string]*calculator.StabilityWindow, ss *sessionState, dropHour, dropMinute int, sl *sessionLogger) {
	for sym := range active {
		snaps := windows[sym].Snapshots
		if len(snaps) > 0 && snaps[len(snaps)-1].IndicatedPrice > 0 {
			continue
		}
		quotes := ss.quoteCount[sym]
		var reason string
		switch {
		case quotes == 0:
			reason = "0 Quotes received — WebSocket subscription may have failed"
		case !ss.hadPrice[sym]:
			reason = fmt.Sprintf("%d Quotes received, EstMatchedPrice always 0 — ATO not formed or Trade messages missing", quotes)
		default:
			reason = fmt.Sprintf("%d Quotes received, had price but reverted to 0", quotes)
		}
		sl.warn(sym, "dropped_no_price", "%s: no indicated price by %02d:%02d ICT, dropping — %s", sym, dropHour, dropMinute, reason)
		delete(active, sym)
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

func renderOrderBooks(views map[string]liveView, now time.Time) {
	if len(views) == 0 {
		return
	}
	syms := make([]string, 0, len(views))
	for sym := range views {
		syms = append(syms, sym)
	}
	sort.Slice(syms, func(i, j int) bool {
		return views[syms[i]].analysis.ImbalanceRatio > views[syms[j]].analysis.ImbalanceRatio
	})
	if len(syms) > obMaxSyms {
		syms = syms[:obMaxSyms]
	}
	fmt.Fprint(os.Stderr, clearTerm)
	fmt.Fprintf(os.Stderr, "%s[orderbook] %s  %d with price (top %d by ratio)%s\n\n",
		ansiBold, now.Format("15:04:05"), len(views), len(syms), ansiReset)
	for _, sym := range syms {
		renderBook(sym, views[sym])
		fmt.Fprintln(os.Stderr)
	}
}

func renderBook(sym string, v liveView) {
	snap := v.snap
	var gapStr string
	if snap.RefPrice > 0 {
		gap := (snap.IndicatedPrice - snap.RefPrice) / snap.RefPrice * 100
		gapStr = fmt.Sprintf(" (%+.2f%% vs ref %.0f)", gap, snap.RefPrice)
	}
	fmt.Fprintf(os.Stderr, "%s%s%s  ● %.0f%s  ratio=%.2fx  score=%d  stable=%d\n",
		ansiBold, sym, ansiReset,
		snap.IndicatedPrice, gapStr,
		v.analysis.ImbalanceRatio, v.score, v.stable)

	asks := snap.AskLevels
	bids := snap.BidLevels
	if len(asks) > obMaxLevels {
		asks = asks[:obMaxLevels]
	}
	if len(bids) > obMaxLevels {
		bids = bids[:obMaxLevels]
	}

	maxVol := 0.0
	for _, l := range asks {
		if l.Volume > maxVol {
			maxVol = l.Volume
		}
	}
	for _, l := range bids {
		if l.Volume > maxVol {
			maxVol = l.Volume
		}
	}

	for i := len(asks) - 1; i >= 0; i-- {
		l := asks[i]
		fmt.Fprintf(os.Stderr, "  %s%9.0f  %9.0f  %s%s\n",
			ansiRed, l.Price, l.Volume, obBar(l.Volume, maxVol), ansiReset)
	}
	fmt.Fprintf(os.Stderr, "  %s─── %.0f%s\n", ansiYellow, snap.IndicatedPrice, ansiReset)
	for _, l := range bids {
		fmt.Fprintf(os.Stderr, "  %s%9.0f  %9.0f  %s%s\n",
			ansiGreen, l.Price, l.Volume, obBar(l.Volume, maxVol), ansiReset)
	}
}

func obBar(vol, maxVol float64) string {
	if maxVol == 0 {
		return ""
	}
	n := int(vol / maxVol * obBarWidth)
	if n < 1 && vol > 0 {
		n = 1
	}
	return strings.Repeat("█", n)
}

func logSessionSummary(fired, active map[string]bool, ss *sessionState, sl *sessionLogger) {
	signalCount := 0
	for _, f := range fired {
		if f {
			signalCount++
		}
	}
	sl.info("", "session_ended", "session ended — monitored: %d  signals fired: %d  no signal: %d",
		len(active), signalCount, len(active)-signalCount)
	for sym := range active {
		if fired[sym] {
			continue
		}
		switch {
		case ss.quoteCount[sym] == 0:
			sl.warn(sym, "no_signal", "%s: no signal — 0 Quotes received (subscription issue?)", sym)
		case !ss.hadPrice[sym]:
			sl.warn(sym, "no_signal", "%s: no signal — %d Quotes, EstMatchedPrice always 0", sym, ss.quoteCount[sym])
		case !ss.seenStable[sym]:
			sl.info(sym, "no_signal", "%s: no signal — had price, never stable (%d Quotes)", sym, ss.quoteCount[sym])
		default:
			if r := ss.gateReason[sym]; r != "" {
				sl.info(sym, "no_signal", "%s: no signal — stable, gate blocked: %s", sym, r)
			} else {
				sl.info(sym, "no_signal", "%s: no signal — was stable, signal window closed before gate passed", sym)
			}
		}
	}
}
