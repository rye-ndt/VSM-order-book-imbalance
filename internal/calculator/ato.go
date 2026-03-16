package calculator

import (
	"sort"
	"time"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
)

type SnapshotAnalysis struct {
	Symbol             string
	CapturedAt         time.Time
	IndicatedPrice     float64
	TotalBidVolume     float64
	TotalAskVolume     float64
	ImbalanceRatio     float64
	BidLevelCount      int
	LargestSingleBid   float64
	LargestBidFraction float64
	HasAskWall         bool
	AskWallPrice       float64
	AskWallVolume      float64
}

type StabilityWindow struct {
	Symbol      string
	Snapshots   []SnapshotAnalysis
	StableCount int
}

// ProcessSnapshot runs the full ATO analysis pipeline for one order book
// snapshot and updates the rolling stability window.
// Returns the analysis and whether all stability conditions hold across the
// configured number of consecutive snapshots.
func ProcessSnapshot(snap input.OrderBookSnapshot, window *StabilityWindow, cfg config.SignalConfig) (SnapshotAnalysis, bool) {
	analysis := SnapshotAnalysis{
		Symbol:         snap.Symbol,
		CapturedAt:     snap.CapturedAt,
		IndicatedPrice: snap.IndicatedPrice,
	}

	analysis.TotalBidVolume, analysis.TotalAskVolume, analysis.ImbalanceRatio = computeImbalanceRatio(snap)

	if analysis.TotalAskVolume == 0 {
		return analysis, false
	}

	analysis.BidLevelCount, analysis.LargestSingleBid, analysis.LargestBidFraction =
		computeBidQuality(snap, analysis.TotalBidVolume)

	analysis.HasAskWall, analysis.AskWallPrice, analysis.AskWallVolume =
		detectAskWall(snap, analysis.TotalAskVolume, cfg)

	isStable := updateStabilityWindow(window, analysis, cfg)

	return analysis, isStable
}

func computeImbalanceRatio(snap input.OrderBookSnapshot) (totalBid, totalAsk, ratio float64) {
	for _, lvl := range snap.BidLevels {
		totalBid += lvl.Volume
	}
	for _, lvl := range snap.AskLevels {
		totalAsk += lvl.Volume
	}
	if totalAsk == 0 {
		return totalBid, 0, 0
	}
	return totalBid, totalAsk, totalBid / totalAsk
}

func computeBidQuality(snap input.OrderBookSnapshot, totalBid float64) (levelCount int, largestSingle, fraction float64) {
	levelCount = len(snap.BidLevels)
	for _, lvl := range snap.BidLevels {
		if lvl.Volume > largestSingle {
			largestSingle = lvl.Volume
		}
	}
	if totalBid > 0 {
		fraction = largestSingle / totalBid
	}
	return
}

func detectAskWall(snap input.OrderBookSnapshot, totalAsk float64, cfg config.SignalConfig) (hasWall bool, wallPrice, wallVolume float64) {
	threshold := totalAsk * cfg.AskWallFraction
	ceiling := snap.IndicatedPrice * cfg.AskWallPriceRange

	sorted := make([]input.PriceLevel, len(snap.AskLevels))
	copy(sorted, snap.AskLevels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Price < sorted[j].Price })

	for _, lvl := range sorted {
		if lvl.Price > ceiling {
			break // sorted ascending — nothing further is within range
		}
		if lvl.Volume >= threshold {
			return true, lvl.Price, lvl.Volume
		}
	}
	return false, 0, 0
}

func updateStabilityWindow(window *StabilityWindow, analysis SnapshotAnalysis, cfg config.SignalConfig) bool {
	window.Snapshots = append(window.Snapshots, analysis)
	if len(window.Snapshots) > cfg.StabilityWindow {
		window.Snapshots = window.Snapshots[1:]
	}

	if len(window.Snapshots) < cfg.StabilityWindow {
		window.StableCount = 0
		return false
	}

	oldest := window.Snapshots[0]
	newest := window.Snapshots[len(window.Snapshots)-1]
	if oldest.IndicatedPrice > 0 {
		drift := (newest.IndicatedPrice - oldest.IndicatedPrice) / oldest.IndicatedPrice
		if drift > 0.01 || drift < -0.01 {
			window.StableCount = 0
			return false
		}
	}

	for _, s := range window.Snapshots {
		if s.ImbalanceRatio < cfg.MinImbalanceRatio ||
			s.LargestBidFraction >= cfg.MaxSpoofFraction ||
			s.BidLevelCount < cfg.MinBidLevels ||
			s.HasAskWall {
			window.StableCount = 0
			return false
		}
	}

	window.StableCount++
	return true
}
