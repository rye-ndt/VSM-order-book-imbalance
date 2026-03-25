package modules

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

const ssiStoreDateFormat = "02/01/2006"

var _ output.MarketStore = (*PostgresMarketStore)(nil)

type PostgresMarketStore struct {
	db *sql.DB
}

func NewPostgresMarketStore(db *sql.DB) *PostgresMarketStore {
	return &PostgresMarketStore{db: db}
}

func (s *PostgresMarketStore) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS stock_ohlcv (
			symbol       TEXT           NOT NULL,
			market       TEXT           NOT NULL,
			trading_date DATE           NOT NULL,
			open         NUMERIC(18, 2) NOT NULL,
			high         NUMERIC(18, 2) NOT NULL,
			low          NUMERIC(18, 2) NOT NULL,
			close        NUMERIC(18, 2) NOT NULL,
			volume       NUMERIC(22, 0) NOT NULL,
			value        NUMERIC(22, 2) NOT NULL,
			created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			PRIMARY KEY (symbol, trading_date)
		);

		CREATE TABLE IF NOT EXISTS stock_foreign_flow (
			symbol       TEXT           NOT NULL,
			trading_date DATE           NOT NULL,
			buy_volume   NUMERIC(22, 0) NOT NULL,
			sell_volume  NUMERIC(22, 0) NOT NULL,
			buy_value    NUMERIC(22, 2) NOT NULL,
			sell_value   NUMERIC(22, 2) NOT NULL,
			net_volume   NUMERIC(22, 0) NOT NULL,
			net_value    NUMERIC(22, 2) NOT NULL,
			created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			PRIMARY KEY (symbol, trading_date)
		);

		CREATE TABLE IF NOT EXISTS index_ohlcv (
			symbol       TEXT           NOT NULL,
			market       TEXT           NOT NULL,
			trading_date DATE           NOT NULL,
			open         NUMERIC(18, 2) NOT NULL,
			high         NUMERIC(18, 2) NOT NULL,
			low          NUMERIC(18, 2) NOT NULL,
			close        NUMERIC(18, 2) NOT NULL,
			volume       NUMERIC(22, 0) NOT NULL,
			value        NUMERIC(22, 2) NOT NULL,
			created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			PRIMARY KEY (symbol, trading_date)
		);

		CREATE TABLE IF NOT EXISTS stock_metrics (
			symbol               TEXT           NOT NULL,
			trading_date         DATE           NOT NULL,
			cpr                  NUMERIC(8, 6)  NOT NULL,
			upper_wick_ratio     NUMERIC(8, 6)  NOT NULL,
			ma20_volume          NUMERIC(22, 0) NOT NULL,
			volume_ratio_1d      NUMERIC(10, 4) NOT NULL,
			volume_trend_3d      TEXT           NOT NULL,
			vpr                  TEXT           NOT NULL,
			momentum_score       SMALLINT       NOT NULL,
			candle_pattern       TEXT           NOT NULL,
			resistance_distance  NUMERIC(10, 6) NOT NULL,
			above_20ma           BOOLEAN        NOT NULL,
			should_monitor_today BOOLEAN        NOT NULL DEFAULT FALSE,
			final_score          SMALLINT       NOT NULL DEFAULT 0,
			position_size_flag   TEXT           NOT NULL DEFAULT 'Skip',
			created_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			updated_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
			PRIMARY KEY (symbol, trading_date)
		);

		ALTER TABLE stock_metrics ADD COLUMN IF NOT EXISTS should_monitor_today BOOLEAN    NOT NULL DEFAULT FALSE;
		ALTER TABLE stock_metrics ADD COLUMN IF NOT EXISTS final_score         SMALLINT   NOT NULL DEFAULT 0;
		ALTER TABLE stock_metrics ADD COLUMN IF NOT EXISTS position_size_flag  TEXT       NOT NULL DEFAULT 'Skip';
		ALTER TABLE stock_metrics ADD COLUMN IF NOT EXISTS ma20_value          NUMERIC(22,2) NOT NULL DEFAULT 0;

		CREATE TABLE IF NOT EXISTS market_regime (
			trading_date DATE        NOT NULL PRIMARY KEY,
			regime       TEXT        NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS signal_log (
			id           BIGSERIAL      PRIMARY KEY,
			symbol       TEXT           NOT NULL,
			final_score  SMALLINT       NOT NULL,
			entry_price  NUMERIC(18, 2) NOT NULL,
			tp_price     NUMERIC(18, 2) NOT NULL,
			fired_at     TIMESTAMPTZ    NOT NULL
		);

		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS indicated_price     NUMERIC(18, 2) NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS open_gap            NUMERIC(10, 6) NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS imbalance_ratio     NUMERIC(10, 4) NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS snapshot_count      SMALLINT       NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS snapshot_fired_at   TIMESTAMPTZ;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS regime              TEXT           NOT NULL DEFAULT 'Choppy';
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS candle_pattern      TEXT           NOT NULL DEFAULT 'Neutral';
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS vpr                 TEXT           NOT NULL DEFAULT 'Neutral';
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS volume_trend        TEXT           NOT NULL DEFAULT 'flat';
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS volume_ratio        NUMERIC(10, 4) NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS momentum_score      SMALLINT       NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS resistance_distance NUMERIC(10, 6) NOT NULL DEFAULT 0;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS above_20ma          BOOLEAN        NOT NULL DEFAULT FALSE;
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS position_size_flag  TEXT           NOT NULL DEFAULT 'Skip';
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d0            NUMERIC(18, 2);
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d1            NUMERIC(18, 2);
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d2            NUMERIC(18, 2);
		ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS entry_price_actual  NUMERIC(18, 2);

		CREATE TABLE IF NOT EXISTS bot_subscribers (
			chat_id       BIGINT      PRIMARY KEY,
			subscribed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS daily_crawl_status (
			trading_date        DATE        NOT NULL PRIMARY KEY,
			market_data_crawled BOOLEAN     NOT NULL DEFAULT FALSE,
			ato_monitored       BOOLEAN     NOT NULL DEFAULT FALSE,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS ato_session_log (
			id           BIGSERIAL   PRIMARY KEY,
			session_date DATE        NOT NULL,
			logged_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			level        TEXT        NOT NULL DEFAULT 'INFO',
			component    TEXT        NOT NULL,
			symbol       TEXT,
			event        TEXT        NOT NULL,
			message      TEXT        NOT NULL
		);
		CREATE INDEX IF NOT EXISTS ato_session_log_session_date_idx ON ato_session_log (session_date);

		ALTER TABLE daily_crawl_status ADD COLUMN IF NOT EXISTS session_summary_sent BOOLEAN NOT NULL DEFAULT FALSE;
	`)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// -------------------------------------------------------------------------
// OHLCV upserts (shared between stock_ohlcv and index_ohlcv)
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) upsertOHLCV(ctx context.Context, table string, records []input.OHLCV) error {
	if len(records) == 0 {
		return nil
	}
	const cols = 9
	args := make([]any, 0, len(records)*cols)
	rows := make([]string, 0, len(records))

	for i, r := range records {
		date, _ := time.Parse(ssiStoreDateFormat, r.TradingDate)
		b := i * cols
		rows = append(rows, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,NOW())",
			b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9,
		))
		args = append(args, r.Symbol, r.Market, date, r.Open, r.High, r.Low, r.Close, r.Volume, r.Value)
	}

	q := fmt.Sprintf(`
		INSERT INTO %s (symbol, market, trading_date, open, high, low, close, volume, value, updated_at)
		VALUES %s
		ON CONFLICT (symbol, trading_date) DO UPDATE SET
			market     = EXCLUDED.market,
			open       = EXCLUDED.open,
			high       = EXCLUDED.high,
			low        = EXCLUDED.low,
			close      = EXCLUDED.close,
			volume     = EXCLUDED.volume,
			value      = EXCLUDED.value,
			updated_at = NOW()
	`, table, strings.Join(rows, ","))

	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func (s *PostgresMarketStore) batchOHLCV(ctx context.Context, table string, records []input.OHLCV) error {
	const batchSize = 500
	for i := 0; i < len(records); i += batchSize {
		end := min(i+batchSize, len(records))
		if err := s.upsertOHLCV(ctx, table, records[i:end]); err != nil {
			return fmt.Errorf("%s batch at %d: %w", table, i, err)
		}
	}
	return nil
}

func (s *PostgresMarketStore) UpsertStockOHLCV(ctx context.Context, records []input.OHLCV) error {
	return s.batchOHLCV(ctx, "stock_ohlcv", records)
}

func (s *PostgresMarketStore) UpsertIndexOHLCV(ctx context.Context, records []input.OHLCV) error {
	return s.batchOHLCV(ctx, "index_ohlcv", records)
}

// -------------------------------------------------------------------------
// Foreign flow upsert
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) UpsertForeignFlow(ctx context.Context, records []input.ForeignFlow) error {
	if len(records) == 0 {
		return nil
	}
	const cols = 8
	const batchSize = 500

	for start := 0; start < len(records); start += batchSize {
		end := min(start+batchSize, len(records))
		batch := records[start:end]

		args := make([]any, 0, len(batch)*cols)
		rows := make([]string, 0, len(batch))

		for i, r := range batch {
			date, _ := time.Parse(ssiStoreDateFormat, r.TradingDate)
			b := i * cols
			rows = append(rows, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,NOW())",
				b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8,
			))
			args = append(args, r.Symbol, date, r.BuyVolume, r.SellVolume, r.BuyValue, r.SellValue, r.NetVolume, r.NetValue)
		}

		q := fmt.Sprintf(`
			INSERT INTO stock_foreign_flow
				(symbol, trading_date, buy_volume, sell_volume, buy_value, sell_value, net_volume, net_value, updated_at)
			VALUES %s
			ON CONFLICT (symbol, trading_date) DO UPDATE SET
				buy_volume  = EXCLUDED.buy_volume,
				sell_volume = EXCLUDED.sell_volume,
				buy_value   = EXCLUDED.buy_value,
				sell_value  = EXCLUDED.sell_value,
				net_volume  = EXCLUDED.net_volume,
				net_value   = EXCLUDED.net_value,
				updated_at  = NOW()
		`, strings.Join(rows, ","))

		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("foreign_flow batch at %d: %w", start, err)
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Stock metrics upsert
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) UpsertStockMetrics(ctx context.Context, records []output.StockMetrics) error {
	if len(records) == 0 {
		return nil
	}
	const cols = 16
	const batchSize = 500

	for start := 0; start < len(records); start += batchSize {
		end := min(start+batchSize, len(records))
		batch := records[start:end]

		args := make([]any, 0, len(batch)*cols)
		rows := make([]string, 0, len(batch))

		for i, m := range batch {
			date, _ := time.Parse(ssiStoreDateFormat, m.TradingDate)
			b := i * cols
			rows = append(rows, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,NOW())",
				b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9, b+10, b+11, b+12, b+13, b+14, b+15, b+16,
			))
			args = append(args,
				m.Symbol, date,
				m.CPR, m.UpperWickRatio,
				m.MA20Volume, m.MA20Value, m.VolumeRatio1D,
				string(m.VolumeTrend3D), string(m.VPR),
				m.MomentumScore, string(m.CandlePattern),
				m.ResistanceDistance, m.Above20MA,
				m.ShouldMonitorToday, m.FinalScore, m.PositionSizeFlag,
			)
		}

		q := fmt.Sprintf(`
			INSERT INTO stock_metrics (
				symbol, trading_date,
				cpr, upper_wick_ratio,
				ma20_volume, ma20_value, volume_ratio_1d,
				volume_trend_3d, vpr,
				momentum_score, candle_pattern,
				resistance_distance, above_20ma,
				should_monitor_today, final_score, position_size_flag,
				updated_at
			) VALUES %s
			ON CONFLICT (symbol, trading_date) DO UPDATE SET
				cpr                  = EXCLUDED.cpr,
				upper_wick_ratio     = EXCLUDED.upper_wick_ratio,
				ma20_volume          = EXCLUDED.ma20_volume,
				ma20_value           = EXCLUDED.ma20_value,
				volume_ratio_1d      = EXCLUDED.volume_ratio_1d,
				volume_trend_3d      = EXCLUDED.volume_trend_3d,
				vpr                  = EXCLUDED.vpr,
				momentum_score       = EXCLUDED.momentum_score,
				candle_pattern       = EXCLUDED.candle_pattern,
				resistance_distance  = EXCLUDED.resistance_distance,
				above_20ma           = EXCLUDED.above_20ma,
				should_monitor_today = EXCLUDED.should_monitor_today,
				final_score          = EXCLUDED.final_score,
				position_size_flag   = EXCLUDED.position_size_flag,
				updated_at           = NOW()
		`, strings.Join(rows, ","))

		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("stock_metrics batch at %d: %w", start, err)
		}
	}
	return nil
}

// -------------------------------------------------------------------------
// Market regime upsert
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) UpsertMarketRegime(ctx context.Context, regime output.MarketRegime) error {
	date, _ := time.Parse(ssiStoreDateFormat, regime.TradingDate)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO market_regime (trading_date, regime, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (trading_date) DO UPDATE SET
			regime     = EXCLUDED.regime,
			updated_at = NOW()
	`, date, string(regime.Regime))
	return err
}

// -------------------------------------------------------------------------
// Foreign net buy + regime loaders (used for scoring inputs)
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) LoadLatestForeignNetBuy(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, net_volume > 0
		FROM stock_foreign_flow
		WHERE trading_date = (SELECT MAX(trading_date) FROM stock_foreign_flow)
	`)
	if err != nil {
		return nil, fmt.Errorf("load latest foreign net buy: %w", err)
	}
	defer rows.Close()

	result := make(map[string]bool)
	for rows.Next() {
		var symbol string
		var netBuy bool
		if err := rows.Scan(&symbol, &netBuy); err != nil {
			return nil, fmt.Errorf("scan foreign net buy: %w", err)
		}
		result[symbol] = netBuy
	}
	return result, rows.Err()
}

func (s *PostgresMarketStore) LoadLatestMarketRegime(ctx context.Context) (output.MarketRegime, bool, error) {
	var tradingDate string
	var regime string
	err := s.db.QueryRowContext(ctx, `
		SELECT TO_CHAR(trading_date, 'DD/MM/YYYY'), regime
		FROM market_regime
		ORDER BY trading_date DESC
		LIMIT 1
	`).Scan(&tradingDate, &regime)
	if err == sql.ErrNoRows {
		return output.MarketRegime{}, false, nil
	}
	if err != nil {
		return output.MarketRegime{}, false, fmt.Errorf("load latest market regime: %w", err)
	}
	return output.MarketRegime{
		TradingDate: tradingDate,
		Regime:      output.RegimeLabel(regime),
	}, true, nil
}

// -------------------------------------------------------------------------
// Latest-date queries
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) latestDate(ctx context.Context, q string, args ...any) (time.Time, bool, error) {
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&t); err != nil {
		return time.Time{}, false, err
	}
	if !t.Valid {
		return time.Time{}, false, nil
	}
	return t.Time, true, nil
}

func (s *PostgresMarketStore) LatestStockOHLCVDate(ctx context.Context) (time.Time, bool, error) {
	return s.latestDate(ctx, `SELECT MAX(trading_date) FROM stock_ohlcv`)
}

func (s *PostgresMarketStore) LatestForeignFlowDate(ctx context.Context) (time.Time, bool, error) {
	return s.latestDate(ctx, `SELECT MAX(trading_date) FROM stock_foreign_flow`)
}

func (s *PostgresMarketStore) LatestIndexOHLCVDate(ctx context.Context, symbol string) (time.Time, bool, error) {
	return s.latestDate(ctx, `SELECT MAX(trading_date) FROM index_ohlcv WHERE symbol = $1`, symbol)
}

// -------------------------------------------------------------------------
// History loaders for metric computation
// -------------------------------------------------------------------------

func (s *PostgresMarketStore) LoadRecentStockOHLCV(ctx context.Context, days int) (map[string][]input.OHLCV, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, market,
		       TO_CHAR(trading_date, 'DD/MM/YYYY'),
		       open, high, low, close, volume, value
		FROM stock_ohlcv
		WHERE trading_date >= CURRENT_DATE - ($1 * INTERVAL '1 day')
		ORDER BY symbol, trading_date DESC
	`, days)
	if err != nil {
		return nil, fmt.Errorf("load recent stock ohlcv: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]input.OHLCV)
	for rows.Next() {
		var r input.OHLCV
		if err := rows.Scan(&r.Symbol, &r.Market, &r.TradingDate,
			&r.Open, &r.High, &r.Low, &r.Close, &r.Volume, &r.Value); err != nil {
			return nil, fmt.Errorf("scan stock ohlcv: %w", err)
		}
		result[r.Symbol] = append(result[r.Symbol], r)
	}
	return result, rows.Err()
}

func (s *PostgresMarketStore) LoadWatchlist(ctx context.Context) ([]output.WatchlistEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			sm.symbol,
			sm.final_score,
			COALESCE(mr.regime, 'Choppy'),
			sm.candle_pattern,
			sm.vpr,
			sm.volume_trend_3d,
			sm.volume_ratio_1d,
			sm.momentum_score,
			sm.resistance_distance,
			sm.above_20ma,
			sm.position_size_flag
		FROM stock_metrics sm
		LEFT JOIN LATERAL (
			SELECT regime FROM market_regime ORDER BY trading_date DESC LIMIT 1
		) mr ON true
		WHERE sm.position_size_flag != 'Skip'
		  AND sm.trading_date = (SELECT MAX(trading_date) FROM stock_metrics)
		  AND sm.symbol !~ '^C[A-Z]+[0-9]{4}$'
		  AND sm.symbol NOT LIKE 'FUE%'
		ORDER BY sm.final_score DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("load watchlist: %w", err)
	}
	defer rows.Close()

	var entries []output.WatchlistEntry
	for rows.Next() {
		var e output.WatchlistEntry
		var regime, candlePattern, vpr, volumeTrend string
		if err := rows.Scan(
			&e.Symbol, &e.FinalScore,
			&regime, &candlePattern, &vpr, &volumeTrend,
			&e.VolumeRatio, &e.MomentumScore, &e.ResistanceDistance,
			&e.Above20MA, &e.PositionSizeFlag,
		); err != nil {
			return nil, fmt.Errorf("scan watchlist entry: %w", err)
		}
		e.Regime = output.RegimeLabel(regime)
		e.CandlePattern = output.CandlePattern(candlePattern)
		e.VPR = output.VPRLabel(vpr)
		e.VolumeTrend = output.VolumeTrend(volumeTrend)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *PostgresMarketStore) LogSignal(ctx context.Context, r output.SignalRecord) error {
	var snapshotFiredAt *time.Time
	if !r.SnapshotFiredAt.IsZero() {
		snapshotFiredAt = &r.SnapshotFiredAt
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO signal_log (
			symbol, final_score, entry_price, tp_price, fired_at,
			indicated_price, open_gap, imbalance_ratio, snapshot_count, snapshot_fired_at,
			regime, candle_pattern, vpr, volume_trend, volume_ratio,
			momentum_score, resistance_distance, above_20ma, position_size_flag
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			$16, $17, $18, $19
		)
	`,
		r.Symbol, r.FinalScore, r.IndicatedPrice, r.TPPrice, r.FiredAt,
		r.IndicatedPrice, r.OpenGap, r.ImbalanceRatio, r.SnapshotCount, snapshotFiredAt,
		string(r.Regime), string(r.CandlePattern), string(r.VPR), string(r.VolumeTrend), r.VolumeRatio,
		r.MomentumScore, r.ResistanceDistance, r.Above20MA, r.PositionSizeFlag,
	)
	return err
}

func (s *PostgresMarketStore) LoadMonitoredStocks(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol
		FROM stock_metrics
		WHERE should_monitor_today = true
		  AND trading_date = (SELECT MAX(trading_date) FROM stock_metrics)
		ORDER BY symbol
	`)
	if err != nil {
		return nil, fmt.Errorf("load monitored stocks: %w", err)
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var sym string
		if err := rows.Scan(&sym); err != nil {
			return nil, fmt.Errorf("scan monitored stock: %w", err)
		}
		symbols = append(symbols, sym)
	}
	return symbols, rows.Err()
}

type TodaySignal struct {
	Symbol          string
	EntryPrice      float64
	TPPrice         float64
	PositionSizeFlag string
	FiredAt         time.Time
}

func (s *PostgresMarketStore) LoadTodaySignals(ctx context.Context) ([]TodaySignal, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, entry_price, tp_price, position_size_flag, fired_at
		FROM signal_log
		WHERE fired_at >= (CURRENT_DATE AT TIME ZONE 'Asia/Ho_Chi_Minh') AT TIME ZONE 'UTC'
		ORDER BY fired_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("load today signals: %w", err)
	}
	defer rows.Close()

	var result []TodaySignal
	for rows.Next() {
		var r TodaySignal
		if err := rows.Scan(&r.Symbol, &r.EntryPrice, &r.TPPrice, &r.PositionSizeFlag, &r.FiredAt); err != nil {
			return nil, fmt.Errorf("scan today signal: %w", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *PostgresMarketStore) AppendSessionLog(ctx context.Context, e output.SessionLogEntry) error {
	var sym *string
	if e.Symbol != "" {
		sym = &e.Symbol
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ato_session_log (session_date, level, component, symbol, event, message)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.SessionDate, e.Level, e.Component, sym, e.Event, e.Message)
	return err
}

func (s *PostgresMarketStore) MarkMarketDataCrawled(ctx context.Context, date time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO daily_crawl_status (trading_date, market_data_crawled, updated_at)
		VALUES ($1::date, true, NOW())
		ON CONFLICT (trading_date) DO UPDATE SET
			market_data_crawled = true,
			updated_at          = NOW()
	`, date)
	return err
}

func (s *PostgresMarketStore) MarkATOMonitored(ctx context.Context, date time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO daily_crawl_status (trading_date, ato_monitored, updated_at)
		VALUES ($1::date, true, NOW())
		ON CONFLICT (trading_date) DO UPDATE SET
			ato_monitored = true,
			updated_at    = NOW()
	`, date)
	return err
}

func (s *PostgresMarketStore) IsTodayMarketDataCrawled(ctx context.Context, date time.Time) (bool, error) {
	var crawled bool
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(
			(SELECT market_data_crawled FROM daily_crawl_status WHERE trading_date = $1::date),
			false
		)
	`, date).Scan(&crawled)
	if err != nil {
		return false, fmt.Errorf("check market data crawled: %w", err)
	}
	return crawled, nil
}

func (s *PostgresMarketStore) IsTodayATOMonitored(ctx context.Context, date time.Time) (bool, error) {
	var monitored bool
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(
			(SELECT ato_monitored FROM daily_crawl_status WHERE trading_date = $1::date),
			false
		)
	`, date).Scan(&monitored)
	if err != nil {
		return false, fmt.Errorf("check ato monitored: %w", err)
	}
	return monitored, nil
}

func (s *PostgresMarketStore) MarkSessionSummarySent(ctx context.Context, date time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO daily_crawl_status (trading_date, session_summary_sent, updated_at)
		VALUES ($1::date, true, NOW())
		ON CONFLICT (trading_date) DO UPDATE SET
			session_summary_sent = true,
			updated_at           = NOW()
	`, date)
	return err
}

func (s *PostgresMarketStore) IsSessionSummarySent(ctx context.Context, date time.Time) (bool, error) {
	var sent bool
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(
			(SELECT session_summary_sent FROM daily_crawl_status WHERE trading_date = $1::date),
			false
		)
	`, date).Scan(&sent)
	if err != nil {
		return false, fmt.Errorf("check session summary sent: %w", err)
	}
	return sent, nil
}

const queryBackfillD0 = `
	UPDATE signal_log s
	SET close_d0 = o.close
	FROM stock_ohlcv o
	WHERE o.symbol       = s.symbol
	  AND o.trading_date = (s.fired_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
	  AND s.close_d0    IS NULL
`

const queryBackfillD1 = `
	UPDATE signal_log s
	SET close_d1 = (
		SELECT close FROM stock_ohlcv
		WHERE symbol       = s.symbol
		  AND trading_date = (
			SELECT MIN(trading_date) FROM stock_ohlcv
			WHERE symbol       = s.symbol
			  AND trading_date > (s.fired_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
		)
	)
	WHERE close_d1 IS NULL
`

const queryBackfillD2 = `
	UPDATE signal_log s
	SET close_d2 = (
		SELECT close FROM stock_ohlcv
		WHERE symbol       = s.symbol
		  AND trading_date = (
			SELECT MIN(trading_date) FROM stock_ohlcv
			WHERE symbol       = s.symbol
			  AND trading_date > (
				SELECT MIN(trading_date) FROM stock_ohlcv
				WHERE symbol       = s.symbol
				  AND trading_date > (s.fired_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
			)
		)
	)
	WHERE close_d2 IS NULL
`

func (s *PostgresMarketStore) BackfillSignalOutcomes(ctx context.Context) (int64, error) {
	var total int64
	for _, q := range []string{queryBackfillD0, queryBackfillD1, queryBackfillD2} {
		res, err := s.db.ExecContext(ctx, q)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

func (s *PostgresMarketStore) LoadTodaySignalSymbols(ctx context.Context, date time.Time) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol
		FROM signal_log
		WHERE (fired_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = $1::date
	`, date)
	if err != nil {
		return nil, fmt.Errorf("load today signal symbols: %w", err)
	}
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var sym string
		if err := rows.Scan(&sym); err != nil {
			return nil, fmt.Errorf("load today signal symbols: scan: %w", err)
		}
		result[sym] = true
	}
	return result, rows.Err()
}

func (s *PostgresMarketStore) AddSubscriber(ctx context.Context, chatID int64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO bot_subscribers (chat_id) VALUES ($1)
		ON CONFLICT (chat_id) DO NOTHING
	`, chatID)
	return err
}

func (s *PostgresMarketStore) RemoveSubscriber(ctx context.Context, chatID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM bot_subscribers WHERE chat_id = $1`, chatID)
	return err
}

func (s *PostgresMarketStore) LoadSubscribers(ctx context.Context) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chat_id FROM bot_subscribers`)
	if err != nil {
		return nil, fmt.Errorf("load subscribers: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan subscriber: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PostgresMarketStore) LoadRecentIndexOHLCV(ctx context.Context, symbol string, days int) ([]input.OHLCV, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, market,
		       TO_CHAR(trading_date, 'DD/MM/YYYY'),
		       open, high, low, close, volume, value
		FROM index_ohlcv
		WHERE symbol = $1
		  AND trading_date >= CURRENT_DATE - ($2 * INTERVAL '1 day')
		ORDER BY trading_date ASC
	`, symbol, days)
	if err != nil {
		return nil, fmt.Errorf("load recent index ohlcv: %w", err)
	}
	defer rows.Close()

	var result []input.OHLCV
	for rows.Next() {
		var r input.OHLCV
		if err := rows.Scan(&r.Symbol, &r.Market, &r.TradingDate,
			&r.Open, &r.High, &r.Low, &r.Close, &r.Volume, &r.Value); err != nil {
			return nil, fmt.Errorf("scan index ohlcv: %w", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
