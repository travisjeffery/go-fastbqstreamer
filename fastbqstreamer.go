package fastbqstreamer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	"github.com/pquerna/ffjson/ffjson"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
)

// Row is a row that will be inserted into BigQuery.
type Row interface {
	DatasetID() string
	TableID() string
	Data() *json.RawMessage
}

// Error contains information about err'd inserts.
type Error struct {
	// Actual error that occurred when inserting.
	Err error

	// Number of rows inserted.
	Size int

	// How long it took to insert to BigQuery.
	Duration time.Duration
}

func (e Error) Error() string {
	return e.Err.Error()
}

// Result contains information about successful inserts.
type Result struct {
	// Number of rows inserted.
	Size int

	// How long it took to insert to BigQuery.
	Duration time.Duration
}

// Options is used to configure the streamer.
type Options struct {
	// The BQ project id to use.
	ProjectID string

	// PEM printable encoding of the private key.
	PEM []byte

	// The email to authenticate with BQ.
	Email string

	// Number of workers.
	Workers int

	// Buffer size.
	BufferSize int

	// Cache size.
	CacheSize int

	// Interval duration to buffer inserts.
	InsertInterval time.Duration

	// Base URL for BigQuery's API. You probably don't need to change this.
	BaseURL string
}

// BQ streams rows and insert them to Google's BigQuery.
type Streamer struct {
	*Options

	service   *bigquery.Service
	cache     *lru.Cache
	successes chan Result
	errors    chan Error
	input     chan Row
	ticker    *time.Ticker
	client    *http.Client
}

// New creates a new BQ using the given options.
func New(opts *Options) (*Streamer, error) {
	token := jwt.NewToken(opts.Email, bigquery.BigqueryScope, opts.PEM)
	transport, err := jwt.NewTransport(token)
	if err != nil {
		return nil, err
	}
	client := transport.Client()

	bigqueryService, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	if opts.Workers == 0 {
		opts.Workers = 4
	}

	if opts.BaseURL == "" {
		opts.BaseURL = "https://www.googleapis.com/bigquery/v2"
	}

	if opts.CacheSize == 0 {
		opts.CacheSize = 50000
	}

	if opts.InsertInterval == 0 {
		opts.InsertInterval = time.Second
	}

	bq := &Streamer{
		Options:   opts,
		service:   bigqueryService,
		errors:    make(chan Error, opts.BufferSize),
		successes: make(chan Result, opts.BufferSize),
		input:     make(chan Row, opts.BufferSize),
		ticker:    time.NewTicker(opts.InsertInterval),
		client:    client,
	}

	cache, err := lru.NewWithEvict(opts.CacheSize, func(k interface{}, value interface{}) {
		go bq.insert(k.(string), value.(*bqinsert))
	})

	if err != nil {
		return nil, err
	}

	bq.cache = cache

	for i := 0; i < opts.Workers; i++ {
		go bq.dispatcher()
	}

	go bq.tick()

	return bq, nil
}

// Close triggers a shutdown, flushing any messages it may have
// buffered. The shutdown has completed when both the Errors and
// Successes channels have been closed. When calling Close, you *must*
// continue to read from those channels in order to drain the results
// of any messages in flight.
func (bq *Streamer) Close() {
	bq.ticker.Stop()
	close(bq.input)
	close(bq.successes)
	close(bq.errors)
}

// Input is the input channel for you to write rows to that you wish
// to send to BQ.
func (bq *Streamer) Input() chan<- Row {
	return bq.input
}

// Successes is the success output channel back to you when a load job
// successfully completes.  You MUST read from this channel or the
// things will deadlock.
func (bq *Streamer) Successes() <-chan Result {
	return bq.successes
}

// Errors is the success output channel back to you when a load job
// fails.  You MUST read from this channel or the things will
// deadlock.
func (bq *Streamer) Errors() <-chan Error {
	return bq.errors
}

func (bq *Streamer) tick() {
	for {
		select {
		case _, ok := <-bq.ticker.C:
			if !ok {
				return
			}

			var wg sync.WaitGroup

			keys := bq.cache.Keys()

			wg.Add(len(keys))

			for _, k := range keys {
				go func(k string) {
					v, ok := bq.cache.Get(k)

					if !ok {
						wg.Done()

						return
					}

					insert := v.(*bqinsert)

					bq.insert(k, insert)

					wg.Done()
				}(k.(string))
			}

			wg.Wait()
		}
	}
}

type bqinsert struct {
	sync.RWMutex

	rows []bqrow
}

type bqrow struct {
	JSON *json.RawMessage `json:"json"`
}

type insertRequest struct {
	Kind            string  `json:"kind"`
	Rows            []bqrow `json:"rows"`
	SkipInvalidRows bool    `json:"skipInvalidRows"`
}

// insert inserts the given rows into BigQuery.
func (bq *Streamer) insert(k string, insert *bqinsert) {
	split := strings.Split(k, "-")
	datasetID := split[0]
	tableID := split[1]

	insert.RLock()
	l := len(insert.rows)
	insert.RUnlock()

	if l <= 0 {
		return
	}

	insert.Lock()
	r := make([]bqrow, len(insert.rows))
	copy(r, insert.rows)
	insert.rows = nil
	insert.Unlock()

	l = len(r)

	now := time.Now()

	req := insertRequest{
		Kind:            "bigquery#tableDataInsertAllRequest",
		Rows:            r,
		SkipInvalidRows: true,
	}

	b, err := ffjson.Marshal(req)

	if err != nil {
		bq.errors <- Error{
			Size:     l,
			Err:      err,
			Duration: time.Since(now),
		}
		return
	}

	buf := bytes.NewBuffer(b)

	resp, err := bq.client.Post(bq.url(datasetID, tableID), "application/json", buf)

	if err != nil {
		bq.errors <- Error{
			Size:     l,
			Err:      err,
			Duration: time.Since(now),
		}
		return
	}

	bq.successes <- Result{
		Size:     l,
		Duration: time.Since(now),
	}

	resp.Body.Close()
}

func (bq *Streamer) url(datasetID, tableID string) string {
	return fmt.Sprintf("%s%s", bq.BaseURL, bq.path(datasetID, tableID))
}

func (bq *Streamer) path(datasetID, tableID string) string {
	return fmt.Sprintf("/projects/%s/datasets/%s/tables/%s/insertAll", bq.ProjectID, datasetID, tableID)
}

// dispatcher reads from the input channel and queues the read rows.
func (bq *Streamer) dispatcher() {
	for r := range bq.input {
		key := fmt.Sprintf("%s-%s", r.DatasetID(), r.TableID())

		v, ok := bq.cache.Get(key)

		var insert *bqinsert

		if ok {
			insert = v.(*bqinsert)
		} else {
			insert = &bqinsert{}

			bq.cache.Add(key, insert)
		}

		insert.Lock()
		insert.rows = append(insert.rows, bqrow{
			JSON: r.Data(),
		})
		insert.Unlock()
	}
}
