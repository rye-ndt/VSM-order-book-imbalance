package calculator

import (
	"math"
	"time"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

// ComputeStockMetrics derives all 8 per-symbol metrics from a slice of candles
// sorted newest-first. Returns false when the slice is empty.
func ComputeStockMetrics(symbol string, candles []input.OHLCV) (output.StockMetrics, bool) {
	if len(candles) == 0 {
		return output.StockMetrics{}, false
	}
	today := candles[0]
	cpr := computeCPR(today)
	ma20vol, ma20val, volRatio, volTrend := computeVolumeMetrics(candles)

	return output.StockMetrics{
		Symbol:             symbol,
		TradingDate:        today.TradingDate,
		CPR:                cpr,
		UpperWickRatio:     computeUpperWickRatio(today),
		MA20Volume:         ma20vol,
		MA20Value:          ma20val,
		VolumeRatio1D:      volRatio,
		VolumeTrend3D:      volTrend,
		VPR:                computeVPR(cpr, volRatio),
		MomentumScore:      computeMomentumScore(candles),
		CandlePattern:      computeCandlePattern(candles),
		ResistanceDistance: computeResistanceDist(candles, today.Close),
		Above20MA:          computeAbove20MA(candles, today.Close),
	}, true
}

// ComputeRegime derives the market regime from daily VNINDEX candles sorted
// oldest-first. Uses 20-week MA + 8-week trend direction (Weinstein method).
func ComputeRegime(daily []input.OHLCV) output.MarketRegime {
	if len(daily) == 0 {
		return output.MarketRegime{Regime: output.RegimeChoppy}
	}
	latest := daily[len(daily)-1]
	return output.MarketRegime{
		TradingDate: latest.TradingDate,
		Regime:      determineRegime(daily),
	}
}

// -------------------------------------------------------------------------
// Individual metric computations
// -------------------------------------------------------------------------

func computeCPR(c input.OHLCV) float64 {
	r := c.High - c.Low
	if r == 0 {
		return 0
	}
	return (c.Close - c.Low) / r
}

func computeUpperWickRatio(c input.OHLCV) float64 {
	r := c.High - c.Low
	if r == 0 {
		return 0
	}
	return (c.High - math.Max(c.Open, c.Close)) / r
}

func computeVolumeMetrics(candles []input.OHLCV) (ma20 float64, ma20val float64, ratio float64, trend output.VolumeTrend) {
	n := min(20, len(candles))
	var sumVol, sumVal float64
	for i := range n {
		sumVol += candles[i].Volume
		sumVal += candles[i].Value
	}
	ma20 = sumVol / float64(n)
	ma20val = sumVal / float64(n)
	if ma20 > 0 {
		ratio = candles[0].Volume / ma20
	}

	if len(candles) >= 3 {
		v0, v1, v2 := candles[0].Volume, candles[1].Volume, candles[2].Volume
		switch {
		case v0 > v1 && v1 > v2:
			trend = output.VolumeTrendRising
		case v0 < v1 && v1 < v2:
			trend = output.VolumeTrendFalling
		default:
			trend = output.VolumeTrendFlat
		}
	} else {
		trend = output.VolumeTrendFlat
	}
	return
}

func computeVPR(cpr, ratio float64) output.VPRLabel {
	switch {
	case cpr >= 0.7 && ratio >= 1.3:
		return output.VPRInstitutional
	case cpr > 0.5 && ratio < 0.8:
		return output.VPRWeak
	case cpr <= 0.3 && ratio >= 1.5:
		return output.VPRDistribution
	case cpr <= 0.3 && ratio < 0.8:
		return output.VPRCapitulation
	default:
		return output.VPRNeutral
	}
}

func computeMomentumScore(candles []input.OHLCV) int {
	n := min(5, len(candles))
	if n == 0 {
		return 0
	}
	window := reverseOHLCV(candles[:n]) // oldest-first

	score := 0
	for _, c := range window {
		cpr := computeCPR(c)
		if cpr >= 0.7 {
			score++
		} else if cpr <= 0.3 {
			score--
		}
	}

	allHigherLows := len(window) >= 2
	for i := 1; i < len(window) && allHigherLows; i++ {
		if window[i].Low <= window[i-1].Low {
			allHigherLows = false
		}
	}
	if allHigherLows {
		score += 2
	}

	for i := 1; i < len(window); i++ {
		if window[i].Open < window[i-1].Low {
			score -= 2
		}
	}
	return score
}

func computeCandlePattern(candles []input.OHLCV) output.CandlePattern {
	if len(candles) == 0 {
		return output.PatternNeutral
	}
	today := candles[0]

	if len(candles) >= 3 {
		// candles newest-first: c3=today, c2=yesterday, c1=2 days ago
		c3, c2, c1 := candles[0], candles[1], candles[2]
		cpr1, cpr2, cpr3 := computeCPR(c1), computeCPR(c2), computeCPR(c3)

		body := func(c input.OHLCV) float64 { return math.Abs(c.Close - c.Open) }

		if c1.Close > c1.Open && c2.Close > c2.Open && c3.Close > c3.Open &&
			c2.Open > c1.Open && c3.Open > c2.Open &&
			cpr1 >= 0.6 && cpr2 >= 0.6 && cpr3 >= 0.6 {
			return output.PatternThreeSoldiers
		}

		if cpr1 >= 0.6 && cpr2 <= 0.4 && cpr3 >= 0.7 && c3.Close > c1.Close {
			return output.PatternDipRecover
		}

		b1, b2, b3 := body(c1), body(c2), body(c3)
		if b1 > b2 && b3 > b2 && c3.Close > c3.Open && b3 > b1*1.5 {
			return output.PatternCompressionBreakout
		}

		if c1.Close < c1.Open && c2.Close < c2.Open && c3.Close < c3.Open &&
			cpr1 <= 0.4 && cpr2 <= 0.4 && cpr3 <= 0.4 {
			return output.PatternThreeCrows
		}

		r3 := c3.High - c3.Low
		if c1.Close > c1.Open && c2.Close > c2.Open &&
			r3 > 0 && (c3.High-math.Max(c3.Open, c3.Close))/r3 > 0.5 {
			return output.PatternShootingStar
		}
	}

	if isHammer(today) {
		return output.PatternHammer
	}
	if isDoji(today) {
		return output.PatternDoji
	}
	return output.PatternNeutral
}

// isHammer detects a hammer candle: long lower wick (>= 2x body), small upper
// wick (<= body), and a body that is not too large relative to the full range.
func isHammer(c input.OHLCV) bool {
	r := c.High - c.Low
	if r == 0 {
		return false
	}
	body := math.Abs(c.Close - c.Open)
	lowerWick := math.Min(c.Open, c.Close) - c.Low
	upperWick := c.High - math.Max(c.Open, c.Close)
	return body > 0 && lowerWick >= 2*body && upperWick <= body && body/r < 0.3
}

// isDoji detects a doji candle: body is less than 5% of the full range.
func isDoji(c input.OHLCV) bool {
	r := c.High - c.Low
	if r == 0 {
		return false
	}
	return math.Abs(c.Close-c.Open)/r < 0.05
}

// ComputeFinalScore scores a stock and returns a position-size flag.
// Returns (0, "Skip") immediately when ShouldMonitorToday is false.
// The regime embedded in m and the thresholds in cfg determine Full/Half/Skip.
func ComputeFinalScore(m output.StockMetrics, cfg config.SignalConfig) (int, string) {
	if !m.ShouldMonitorToday {
		return 0, "Skip"
	}

	score := 0

	// CPR
	switch {
	case m.CPR >= 0.7:
		score += 2
	case m.CPR < 0.5:
		score -= 1
	}

	// Upper Wick Ratio (check tightest bound first)
	switch {
	case m.UpperWickRatio > 0.40:
		score -= 2
	case m.UpperWickRatio > 0.30:
		score -= 1
	case m.UpperWickRatio < 0.10:
		score += 1
	}

	// Volume Ratio vs MA20
	switch {
	case m.VolumeRatio1D >= 2.0:
		score += 3
	case m.VolumeRatio1D >= 1.5:
		score += 2
	case m.VolumeRatio1D >= 1.2:
		score += 1
	case m.VolumeRatio1D < 0.8:
		score -= 1
	}

	// Volume Trend (last 3 days)
	switch m.VolumeTrend3D {
	case output.VolumeTrendRising:
		score += 2
	case output.VolumeTrendFalling:
		score -= 1
	}

	// VPR Label
	switch m.VPR {
	case output.VPRInstitutional:
		score += 3
	case output.VPRWeak:
		score -= 2
	case output.VPRDistribution:
		score -= 3
	}

	// Momentum Score
	switch {
	case m.MomentumScore >= 5:
		score += 3
	case m.MomentumScore >= 4:
		score += 2
	case m.MomentumScore >= 2:
		score += 1
	case m.MomentumScore <= -2:
		score -= 3
	case m.MomentumScore <= 0:
		score -= 2
	}

	// Candle Pattern
	switch m.CandlePattern {
	case output.PatternThreeSoldiers, output.PatternDipRecover:
		score += 3
	case output.PatternCompressionBreakout:
		score += 2
	case output.PatternHammer:
		score += 1
	case output.PatternShootingStar:
		score -= 3
	case output.PatternThreeCrows:
		score -= 4
	// Doji and Neutral → +0 (default, no action)
	}

	// Resistance Distance
	switch {
	case m.ResistanceDistance > 0.06:
		score += 3
	case m.ResistanceDistance > 0.04:
		score += 2
	case m.ResistanceDistance > 0.02:
		score += 1
	case m.ResistanceDistance > 0.01:
		score -= 2
	default:
		score -= 3
	}

	// Above 20MA
	if m.Above20MA {
		score += 1
	}

	// Foreign Net Buy
	if m.ForeignNetBuy {
		score += 1
	}

	// Regime-adjusted thresholds
	fullThreshold, halfThreshold := cfg.ChoppyFullScore, cfg.ChoppyHalfScore
	switch m.Regime {
	case output.RegimeBull:
		fullThreshold, halfThreshold = cfg.BullFullScore, cfg.BullHalfScore
	case output.RegimeBear:
		fullThreshold, halfThreshold = cfg.BearFullScore, cfg.BearHalfScore
	}

	switch {
	case score >= fullThreshold:
		return score, "Full"
	case score >= halfThreshold:
		return score, "Half"
	default:
		return score, "Skip"
	}
}

// RegimeScoreThreshold returns the minimum FinalScore for a signal to fire
// in the morning session. These are the half-position thresholds from
// ComputeFinalScore, re-expressed here for the ATO signal gate.
func RegimeScoreThreshold(regime output.RegimeLabel, cfg config.SignalConfig) int {
	switch regime {
	case output.RegimeBull:
		return cfg.BullHalfScore
	case output.RegimeBear:
		return cfg.BearHalfScore
	default: // Choppy
		return cfg.ChoppyHalfScore
	}
}

// ShouldMonitorToday returns true when a stock passes all daily screening
// criteria:
//   - Bullish setup: CPR >= 0.7 OR hammer/doji pattern
//   - No bearish pattern: not Three_Crows (bearish engulfing proxy) or Shooting_Star
//   - Volume ratio >= 1.2x MA20
//   - VPR not Distribution
//   - MomentumScore >= 1
func ShouldMonitorToday(m output.StockMetrics, minMA20Value float64) bool {
	bullishSetup := m.CPR >= 0.7 ||
		m.CandlePattern == output.PatternHammer ||
		m.CandlePattern == output.PatternDoji

	noBearish := m.CandlePattern != output.PatternThreeCrows &&
		m.CandlePattern != output.PatternShootingStar

	return bullishSetup &&
		noBearish &&
		m.VolumeRatio1D >= 1.2 &&
		m.VPR != output.VPRDistribution &&
		m.MomentumScore >= 1 &&
		m.MA20Value >= minMA20Value
}

func computeResistanceDist(candles []input.OHLCV, close float64) float64 {
	n := min(20, len(candles))
	if n < 5 {
		return 0.10
	}
	window := reverseOHLCV(candles[:n]) // oldest-first for index continuity

	nearest := math.MaxFloat64
	for i := 2; i < n-2; i++ {
		h := window[i].High
		if h > window[i-1].High && h > window[i-2].High &&
			h > window[i+1].High && h > window[i+2].High &&
			h > close && h < nearest {
			nearest = h
		}
	}
	if nearest == math.MaxFloat64 {
		return 0.10
	}
	return (nearest - close) / close
}

func computeAbove20MA(candles []input.OHLCV, todayClose float64) bool {
	n := min(20, len(candles))
	var sum float64
	for i := range n {
		sum += candles[i].Close
	}
	return todayClose > sum/float64(n)
}

// -------------------------------------------------------------------------
// Regime helpers
// -------------------------------------------------------------------------

func determineRegime(daily []input.OHLCV) output.RegimeLabel {
	weekly := deriveWeeklyCloses(daily)
	if len(weekly) < 2 {
		return output.RegimeChoppy
	}

	n := min(20, len(weekly))
	recent := weekly[len(weekly)-n:]

	var sum float64
	for _, c := range recent {
		sum += c
	}
	ma20w := sum / float64(len(recent))
	current := recent[len(recent)-1]

	last8 := recent
	if len(recent) > 8 {
		last8 = recent[len(recent)-8:]
	}

	higher, lower := 0, 0
	for i := 1; i < len(last8); i++ {
		if last8[i] > last8[i-1] {
			higher++
		} else if last8[i] < last8[i-1] {
			lower++
		}
	}
	total := len(last8) - 1

	if current > ma20w && higher > total/2 {
		return output.RegimeBull
	}
	if current < ma20w && lower > total/2 {
		return output.RegimeBear
	}
	return output.RegimeChoppy
}

// deriveWeeklyCloses groups daily candles by ISO year-week and returns the
// closing price of the last trading day of each week, oldest-first.
func deriveWeeklyCloses(daily []input.OHLCV) []float64 {
	type weekKey struct{ year, week int }
	var order []weekKey
	seen := make(map[weekKey]bool)
	closes := make(map[weekKey]float64)

	for _, c := range daily {
		t, err := time.Parse("02/01/2006", c.TradingDate)
		if err != nil {
			continue
		}
		yr, wk := t.ISOWeek()
		k := weekKey{yr, wk}
		if !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
		closes[k] = c.Close // overwrite; daily is oldest→newest so last write = Friday or last trading day
	}

	result := make([]float64, len(order))
	for i, k := range order {
		result[i] = closes[k]
	}
	return result
}

func reverseOHLCV(s []input.OHLCV) []input.OHLCV {
	out := make([]input.OHLCV, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}
