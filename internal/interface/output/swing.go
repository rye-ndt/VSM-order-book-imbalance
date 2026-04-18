package output

import "time"

type SwingSignalRecord struct {
	Symbol             string
	SignalDate         time.Time
	Regime             RegimeLabel
	FinalScore         int
	PositionSizeFlag   string
	CandlePattern      CandlePattern
	VPR                VPRLabel
	VolumeTrend        VolumeTrend
	VolumeRatio        float64
	MomentumScore      int
	ResistanceDistance float64
	Above20MA          bool
	ForeignNetBuy      bool
	EntryPrice         float64
	SLPrice            float64
	TPPrice            float64
}

type SwingBroadcastRecord struct {
	SignalDate     time.Time
	Regime         string
	ScoreThreshold int
	TotalWatchlist int
	Stocks         []SwingSignalRecord
}
