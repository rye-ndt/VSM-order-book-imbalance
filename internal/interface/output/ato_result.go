package output

import "time"

type ATODailyResult struct {
	SessionDate           time.Time
	Symbol                string
	Regime                string
	FinalScore            int
	PositionSizeFlag      string
	HadIndicatedPrice     bool
	QuoteCount            int
	EarlyRatio            float64
	PeakImbalanceRatio    float64
	FinalIndicatedPrice   float64
	RefPrice              float64
	CeilPrice             float64
	SellWarnFired         bool
	StableSnapshotsAtFire int
	SignalFired           bool
	GateBlockReason       string
	DropReason            string
}
