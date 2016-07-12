package fastbqstreamer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gohttp/response"
	"github.com/segmentio/go-log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	datasetID = os.Getenv("DATASET_ID")
	tableID   = os.Getenv("TABLE_ID")
)

type StreamerTestSuite struct {
	suite.Suite
	mux     *http.ServeMux
	service *Streamer
	server  *httptest.Server
}

func (suite *StreamerTestSuite) SetupTest() {
	suite.mux = http.NewServeMux()
	suite.server = httptest.NewServer(suite.mux)

	service, err := New(&Options{
		ProjectID:     os.Getenv("PROJECT_ID"),
		Email:         os.Getenv("EMAIL"),
		PEM:           []byte(os.Getenv("PEM")),
		BaseURL:       suite.server.URL,
		CacheInterval: time.Millisecond,
	})
	log.Check(err)

	suite.service = service
}

func (suite *StreamerTestSuite) TeardownTest() {
	suite.service.Close()
}

type row struct {
	datasetID string
	tableID   string
	data      *json.RawMessage
}

func (r row) DatasetID() string {
	return r.datasetID
}

func (r row) TableID() string {
	return r.tableID
}

func (r row) Data() *json.RawMessage {
	return r.data
}

func (suite *StreamerTestSuite) TestInsert() {
	suite.mux.HandleFunc(suite.service.path(datasetID, tableID), func(w http.ResponseWriter, r *http.Request) {
		response.OK(w)
	})

	l := 5

	for i := 0; i < 5; i++ {
		now := time.Now()

		j := json.RawMessage(fmt.Sprintf(`{
					"id":   "user-%d",
					"date": "%d"
				}`, i, now.Unix()))

		suite.service.Input() <- row{
			datasetID: datasetID,
			tableID:   tableID,
			data:      &j,
		}
	}

	for {
		select {
		case r := <-suite.service.Successes():
			assert.NotNil(suite.T(), r)
			assert.Equal(suite.T(), l, r.Size)
			return
		case err := <-suite.service.Errors():
			assert.Nil(suite.T(), err)
			return
		}
	}
}

func TestStreamerTestSuite(t *testing.T) {
	suite.Run(t, new(StreamerTestSuite))
}
