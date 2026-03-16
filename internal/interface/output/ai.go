package output

import "context"

type AI interface {
	Response(ctx context.Context, text string) (string, error)
	Interpret(ctx context.Context, rec SignalRecord, scoreThreshold int) (SignalInterpretation, error)
	XInterpret(ctx context.Context, interp SignalInterpretation) (string, error)
	TelegramInterpret(ctx context.Context, interp SignalInterpretation) (string, error)
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
