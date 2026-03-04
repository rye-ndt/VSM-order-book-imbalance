package job

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/example/order-book-imbalance/internal/interface/input"
)

// fetchTimeout caps how long a single job run may block.
const fetchTimeout = 2 * time.Hour

// MarketDataJob fetches daily market snapshots from the stock data source.
// It satisfies the cron.Job interface via its Run method.
//
// The job receives a StockDataClient through its constructor, so any
// implementation of input.StockDataClient can be injected without modifying
// this file – swap the adapter in main and the job is unaffected.
type MarketDataJob struct {
	client input.StockDataClient
}

// NewMarketDataJob constructs a MarketDataJob backed by the given client.
// Inject any implementation of input.StockDataClient here.
func NewMarketDataJob(client input.StockDataClient) *MarketDataJob {
	return &MarketDataJob{client: client}
}

// Run executes all three data fetches concurrently and logs a summary.
// It is invoked by the cron scheduler and satisfies the cron.Job interface.
func (j *MarketDataJob) Run() {
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		records, err := j.client.FetchAllStocksOHLCV(ctx)
		if err != nil {
			log.Printf("[market_data_job] FetchAllStocksOHLCV error: %v", err)
			return
		}
		log.Printf("[market_data_job] FetchAllStocksOHLCV: %d records", len(records))
	}()

	go func() {
		defer wg.Done()
		records, err := j.client.FetchForeignFlow(ctx)
		if err != nil {
			log.Printf("[market_data_job] FetchForeignFlow error: %v", err)
			return
		}
		log.Printf("[market_data_job] FetchForeignFlow: %d records", len(records))
	}()

	go func() {
		defer wg.Done()
		records, err := j.client.FetchVNIndexOHLCV(ctx, 20)
		if err != nil {
			log.Printf("[market_data_job] FetchVNIndexOHLCV error: %v", err)
			return
		}
		log.Printf("[market_data_job] FetchVNIndexOHLCV: %d records", len(records))
	}()

	wg.Wait()
}
