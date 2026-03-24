package output

import (
	"context"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
)

type AI interface {
	Response(ctx context.Context, text string) (string, error)
	Interpret(ctx context.Context, rec SignalRecord, scoreThreshold int) (SignalInterpretation, error)
	XInterpret(ctx context.Context, interp SignalInterpretation) (string, error)
	TelegramInterpret(ctx context.Context, interp SignalInterpretation) (string, error)
	SummarizeSession(ctx context.Context, rec SessionSummaryRecord) (string, error)
	WarnSellPressure(ctx context.Context, rec SellWarnRecord) (string, error)
}

type SymbolSessionOutcome struct {
	Symbol          string
	FinalScore      int
	PositionSize    string
	CandlePattern   string
	VPR             string
	MomentumScore   int
	PeakRatio       float64
	IndicatedPrice  float64
	RefPrice        float64
	SignalFired     bool
	Dropped         bool
	DropReason      string
	GateBlockReason string
}

type SessionSummaryRecord struct {
	SessionDate    time.Time
	Regime         string
	ScoreThreshold int
	Outcomes       []SymbolSessionOutcome
	SignalCount     int
}

type SellWarnRecord struct {
	Symbol         string
	SessionDate    time.Time
	WarnedAt       time.Time
	IndicatedPrice float64
	RefPrice       float64
	CeilingPrice   float64
	ImbalanceRatio float64
	TopAskLevels   []input.PriceLevel
	TopBidLevels   []input.PriceLevel
	FinalScore     int
	Regime         string
	CandlePattern  string
	VPR            string
	MomentumScore  int
}

type SignalInterpretation struct {
	Recommendation string          `json:"recommendation"`
	Confidence     int             `json:"confidence"`
	SignalStrength string          `json:"signal_strength"`
	EntryPrice     float64         `json:"entry_price"`
	TPPrice        float64         `json:"tp_price"`
	SLPrice        float64         `json:"sl_price"`
	DataUsed       []string        `json:"data_used"`
	Reasoning      SignalReasoning `json:"reasoning"`
	T2Note         string          `json:"t2_note"`
	ActionClarity  string          `json:"action_clarity"`
	AvoidIf        string          `json:"avoid_if"`
}

type SignalReasoning struct {
	OrderBook  string `json:"order_book"`
	Technicals string `json:"technicals"`
	RegimeFit  string `json:"regime_fit"`
	KeyRisk    string `json:"key_risk"`
}
