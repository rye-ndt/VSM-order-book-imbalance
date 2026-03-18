package output

import (
	"context"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
)

// SignalRecord is written to signal_log whenever an ATO signal fires.
// It captures a full feature snapshot (Layer 1 of the event study dataset) so
// that every observable at signal time is preserved alongside the outcome data
// that will be back-filled once T+1 and T+2 prices are available.
type SignalRecord struct {
	Symbol         string
	FinalScore     int
	IndicatedPrice float64 // ATO estimated clearing price; also written to entry_price at fire time
	TPPrice        float64
	FiredAt        time.Time

	// Order-book snapshot at the moment the signal fired.
	OpenGap         float64   // (indicated_price - ref_price) / ref_price
	ImbalanceRatio  float64   // bid/ask volume ratio from the triggering snapshot
	SnapshotCount   int       // consecutive stable snapshots at fire time (sanity check; should be 3)
	SnapshotFiredAt time.Time // CapturedAt timestamp of the triggering snapshot

	// Nightly metrics snapshotted from stock_metrics / market_regime at fire time.
	Regime             RegimeLabel
	CandlePattern      CandlePattern
	VPR                VPRLabel
	VolumeTrend        VolumeTrend
	VolumeRatio        float64
	MomentumScore      int
	ResistanceDistance float64
	Above20MA          bool
	PositionSizeFlag   string
}

type VolumeTrend string

const (
	VolumeTrendRising  VolumeTrend = "rising"
	VolumeTrendFalling VolumeTrend = "falling"
	VolumeTrendFlat    VolumeTrend = "flat"
)

type VPRLabel string

const (
	VPRInstitutional VPRLabel = "Institutional"
	VPRWeak          VPRLabel = "Weak"
	VPRDistribution  VPRLabel = "Distribution"
	VPRCapitulation  VPRLabel = "Capitulation"
	VPRNeutral       VPRLabel = "Neutral"
)

type CandlePattern string

const (
	PatternThreeSoldiers       CandlePattern = "Three_Soldiers"
	PatternDipRecover          CandlePattern = "Dip_Recover"
	PatternCompressionBreakout CandlePattern = "Compression_Breakout"
	PatternThreeCrows          CandlePattern = "Three_Crows"
	PatternShootingStar        CandlePattern = "Shooting_Star"
	PatternHammer              CandlePattern = "Hammer"
	PatternDoji                CandlePattern = "Doji"
	PatternNeutral             CandlePattern = "Neutral"
)

type RegimeLabel string

const (
	RegimeBull   RegimeLabel = "Bull"
	RegimeBear   RegimeLabel = "Bear"
	RegimeChoppy RegimeLabel = "Choppy"
)

type StockMetrics struct {
	Symbol             string
	TradingDate        string // DD/MM/YYYY
	CPR                float64
	UpperWickRatio     float64
	MA20Volume         float64
	MA20Value          float64
	VolumeRatio1D      float64
	VolumeTrend3D      VolumeTrend
	VPR                VPRLabel
	MomentumScore      int
	CandlePattern      CandlePattern
	ResistanceDistance float64
	Above20MA          bool
	ShouldMonitorToday bool
	// Scoring inputs — populated in the pipeline before ComputeFinalScore.
	// ForeignNetBuy and Regime are not stored in stock_metrics; they are
	// loaded from their own tables and embedded here for self-contained scoring.
	ForeignNetBuy bool
	Regime        RegimeLabel
	// Scoring outputs — stored in stock_metrics.
	FinalScore       int
	PositionSizeFlag string
}

type MarketRegime struct {
	TradingDate string // DD/MM/YYYY
	Regime      RegimeLabel
}

type SessionLogEntry struct {
	SessionDate time.Time
	Level       string // INFO, WARN, ERROR
	Component   string // e.g. "ato"
	Symbol      string // empty if not symbol-specific
	Event       string // machine-readable e.g. "first_quote", "gate_blocked"
	Message     string
}

type MarketStore interface {
	Migrate(ctx context.Context) error

	// AppendSessionLog writes a single append-only log entry for an ATO session
	// so that post-session analysis can be done even if the process crashed.
	AppendSessionLog(ctx context.Context, e SessionLogEntry) error

	UpsertStockOHLCV(ctx context.Context, records []input.OHLCV) error
	UpsertForeignFlow(ctx context.Context, records []input.ForeignFlow) error
	UpsertIndexOHLCV(ctx context.Context, records []input.OHLCV) error
	UpsertStockMetrics(ctx context.Context, records []StockMetrics) error
	UpsertMarketRegime(ctx context.Context, regime MarketRegime) error

	LatestStockOHLCVDate(ctx context.Context) (time.Time, bool, error)
	LatestForeignFlowDate(ctx context.Context) (time.Time, bool, error)
	LatestIndexOHLCVDate(ctx context.Context, symbol string) (time.Time, bool, error)

	// LoadRecentStockOHLCV returns the last N calendar days of equity OHLCV
	// grouped by symbol, sorted newest-first per symbol.
	LoadRecentStockOHLCV(ctx context.Context, days int) (map[string][]input.OHLCV, error)

	// LoadRecentIndexOHLCV returns the last N calendar days of index OHLCV
	// for the given symbol, sorted oldest-first (needed for weekly derivation).
	LoadRecentIndexOHLCV(ctx context.Context, symbol string, days int) ([]input.OHLCV, error)

	// LoadLatestForeignNetBuy returns symbol → (net_volume > 0) for the most
	// recent trading date available in stock_foreign_flow.
	LoadLatestForeignNetBuy(ctx context.Context) (map[string]bool, error)

	// LoadLatestMarketRegime returns the most recently stored market regime.
	LoadLatestMarketRegime(ctx context.Context) (MarketRegime, bool, error)

	// LoadMonitoredStocks returns the symbols marked should_monitor_today = true
	// for the most recent trading date in stock_metrics.
	LoadMonitoredStocks(ctx context.Context) ([]string, error)

	// LoadWatchlist returns all symbols with position_size_flag != 'Skip' on
	// the most recent trading date, along with their pre-computed final scores.
	// This is the authoritative morning watchlist: nightly scoring and regime
	// thresholds are already baked into position_size_flag.
	LoadWatchlist(ctx context.Context) ([]WatchlistEntry, error)

	// LogSignal persists a signal_log row when an ATO signal fires.
	LogSignal(ctx context.Context, r SignalRecord) error

	// MarkMarketDataCrawled records that the nightly market data pipeline ran
	// successfully for the given calendar date.
	MarkMarketDataCrawled(ctx context.Context, date time.Time) error

	// MarkATOMonitored records that the ATO monitoring session was started for
	// the given calendar date.
	MarkATOMonitored(ctx context.Context, date time.Time) error

	// IsTodayMarketDataCrawled reports whether the nightly pipeline has already
	// been marked as completed for the given date.
	IsTodayMarketDataCrawled(ctx context.Context, date time.Time) (bool, error)

	// IsTodayATOMonitored reports whether an ATO session has already been
	// started for the given date.
	IsTodayATOMonitored(ctx context.Context, date time.Time) (bool, error)
}

// WatchlistEntry is one row from the morning watchlist query.
// All metric fields are snapshotted here at session start so pollOnce can
// populate SignalRecord without any DB round-trip during the hot polling loop.
type WatchlistEntry struct {
	Symbol             string
	FinalScore         int
	Regime             RegimeLabel
	CandlePattern      CandlePattern
	VPR                VPRLabel
	VolumeTrend        VolumeTrend
	VolumeRatio        float64
	MomentumScore      int
	ResistanceDistance float64
	Above20MA          bool
	PositionSizeFlag   string
}
