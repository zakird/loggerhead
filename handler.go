package loggerhead

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics
var (
	// Outcomes of logging requests
	requestResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "add_chain_outcome_total",
			Help: "Number of requests with each outcome.",
		},
		[]string{"outcome"},
	)

	// Overall handler execution time
	handlerTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_handler_time_seconds",
		Help:    "The overall time for the log HTTP handler to return.",
		Buckets: prometheus.LinearBuckets(0, 0.05, 100),
	})

	// DB interaction time
	transactionTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "transaction_time_seconds",
		Help:    "The time the DB transaction was active.",
		Buckets: prometheus.LinearBuckets(0, 0.05, 100),
	})

	// Update time (exclusive of DB interaction)
	updateTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "update_time_seconds",
		Help:    "The time this node spent processing DB results before returning.",
		Buckets: prometheus.LinearBuckets(0, 0.005, 100),
	})
)

func init() {
	prometheus.MustRegister(requestResult)
	prometheus.MustRegister(handlerTime)
	prometheus.MustRegister(transactionTime)
	prometheus.MustRegister(updateTime)
}

type LogHandler struct {
	Client *as.Client
	dbLock sync.Mutex
}

type addChainRequest struct {
	Chain []string `json:"chain"`
}

var (
	outcomeBodyReadErr      = "body-read-err"
	outcomeJSONParseErr     = "json-parse-err"
	outcomeEmptyChainErr    = "empty-chain"
	outcomeBase64DecodeErr  = "base64-decode-err"
	outcomeDBLockTimeout    = "db-lock-timeout"
	outcomeTxBeginErr       = "tx-begin-err"
	outcomeReadFrontierErr  = "read-frontier-err"
	outcomeLogCertErr       = "log-cert-err"
	outcomeWriteFrontierErr = "write-frontier-err"
	outcomeTxCommitErr      = "tx-commit-err"
	outcomeSuccess          = "success"
	couldNotLockDB          = "could-not-lock-db"
	outcomeLogFrontierErr   = "write-frontier-err"

	responseValues = map[string]struct {
		Status  int
		Message string
	}{
		outcomeBodyReadErr:      {http.StatusBadRequest, "Failed to read body: %v"},
		outcomeJSONParseErr:     {http.StatusBadRequest, "Failed to parse body: %v"},
		outcomeEmptyChainErr:    {http.StatusBadRequest, "No certificates provided in body: %v"},
		outcomeBase64DecodeErr:  {http.StatusBadRequest, "Base64 decoding failed: %v"},
		outcomeDBLockTimeout:    {http.StatusInternalServerError, "Timed out waiting for DB: %v"},
		outcomeTxBeginErr:       {http.StatusInternalServerError, "Could not get DB transaction: %v"},
		outcomeReadFrontierErr:  {http.StatusInternalServerError, "Failed to fetch frontier: %v"},
		outcomeLogCertErr:       {http.StatusInternalServerError, "Failed to log certificate: %v"},
		outcomeWriteFrontierErr: {http.StatusInternalServerError, "Failed to write frontier: %v"},
		outcomeTxCommitErr:      {http.StatusInternalServerError, "Failed to commit changes: %v"},
		outcomeSuccess:          {http.StatusOK, "success: %v"},
	}
)

// two "tables"
//
// 1. certificates{treeSize int [key] -> certificate []byte, frontier []byte}
//
//    This is the master table and the only one that truly needs to remain consistent.
//    Everytime we get a new certificate, we append a record to the table. The database
//    maintains consistency because there is key-level atomicity (which we can enforce
//    same treeSize. If anything goes horribly astray, we restore cluster state based
//    on the last record in this table. This requires a map reduce, but meh, it really
//    shouldn't ever happen except on server boot, or if a failure somewhere
//    (e.g., power loss between writing to certificates and the frontier)
//
// 1. frontier{id int [key](always 1) -> locked bool, lockedAt datetime, frontier []byte}
//
//    Unfortunately, if we only relied on this this constraint, there'd potentially
//    be a *ton* of contention between multiple frontend servers. To prevent this,
//	  we have a second helper table that contains the "frontier" and tree size. We
//    write to this *after* we write to certificates and this write is *not* inside
//    of a transaction. Therefore, it's possible for this table to enter a stale state.
//    When this happens, we have to refresh it based on what's in the certificates table

func (lh *LogHandler) readFrontier() (frontier, uint32, error) {
	// set lock bit in frontier table. if cannot set, keep trying using exponential
	// backoff. If we can't get a lock in 10 seconds, something is probably wrong, give up.
	// once lock is achieved, figure out what treeSize slot we can use.
	key, err := as.NewKey("ct", "frontier", 0)
	if err != nil {
		panic(err)
	}
	readPolicy := as.NewPolicy()
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	var rec *as.Record
	for {
		rec, err := lh.Client.Get(readPolicy, key)
		if err != nil {
			panic(err)
		}
		if rec != nil && rec.Bins["locked"].(int) != 0 {
			continue
		}
		// woo! we're not locked! lock database.
		writePolicy.Generation = rec.Generation
		bin := as.NewBin("locked", 1)
		lh.Client.PutBins(writePolicy, key, bin)
	}
	f := frontier{}
	if rec != nil {
		err = f.Unmarshal(rec.Bins["frontier"].([]byte))
	}
	return f, rec.Generation, err
}

func (lh *LogHandler) logCertificate(timestamp int64, treeSize int64, cert []byte, f frontier) error {
	writePolicy := as.NewWritePolicy(0, 0)
	// make sure we don't ever overwrite an existing record due to a lock failure.
	// this is an append only data structure.
	writePolicy.RecordExistsAction = as.CREATE_ONLY
	certBin := as.NewBin("certificate", cert)
	frontBin := as.NewBin("frontier", f.Marshal())
	key, err := as.NewKey("ct", "frontier", treeSize)
	err = lh.Client.PutBins(writePolicy, key, certBin, frontBin)
	return err
}

func (lh *LogHandler) logFrontier(f frontier, generation uint32) error {
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.Generation = generation
	frontBin := as.NewBin("frontier", f.Marshal())
	lockBin := as.NewBin("locked", 0)
	key, err := as.NewKey("ct", "frontier", 0)
	err = lh.Client.PutBins(writePolicy, key, frontBin, lockBin)
	return err
}

func (lh *LogHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	outcome := outcomeSuccess
	err := error(nil)
	treeSize := int64(0)
	enterHandler := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitHandler := float64(time.Now().UnixNano()) / 1000000000.0

		elapsed := exitHandler - enterHandler
		status := responseValues[outcome].Status
		message := fmt.Sprintf(responseValues[outcome].Message, err)

		handlerTime.Observe(elapsed)
		requestResult.With(prometheus.Labels{"outcome": outcome}).Inc()

		response.WriteHeader(status)
		response.Write([]byte(message + "\n"))
		log.Printf("[%03d] [%d] [%8.6f] %s", status, treeSize, elapsed, message)
	}()

	// Extract certificate from request
	// XXX: No verification of input certificate
	//  - No check that it parses as valid X.509
	//  - No verification of the certificate chain
	//  - No deduplication
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		outcome = outcomeBodyReadErr
		return
	}
	ctRequest := addChainRequest{}
	err = json.Unmarshal(body, &ctRequest)
	if err != nil {
		outcome = outcomeJSONParseErr
		return
	}
	if len(ctRequest.Chain) == 0 {
		outcome = outcomeEmptyChainErr
		return
	}
	cert, err := base64.StdEncoding.DecodeString(ctRequest.Chain[0])
	if err != nil {
		outcome = outcomeBase64DecodeErr
		return
	}

	// Await access to the DB (local per-process lock only)
	gotLock := make(chan bool, 1)
	cancelled := make(chan bool, 1)
	go func() {
		lh.dbLock.Lock()
		gotLock <- true
		if <-cancelled {
			lh.dbLock.Unlock()
		}
	}()

	select {
	case <-gotLock:
	case <-time.After(500 * time.Millisecond):
		cancelled <- true
		outcome = outcomeDBLockTimeout
		return
	}
	defer lh.dbLock.Unlock()

	enterTx := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitTx := float64(time.Now().UnixNano()) / 1000000000.0
		transactionTime.Observe(exitTx - enterTx)
	}()

	// Get the frontier from the DB
	f, gen, err := lh.readFrontier()
	if err != nil {
		outcome = outcomeReadFrontierErr
		return
	}
	// Update the frontier with this certificate
	enterUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	f.Add(cert)
	treeSize = int64(f.Size())
	exitUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	updateTime.Observe(exitUpdate - enterUpdate)
	// Log the certificate
	timestamp := time.Now().Unix()
	err = lh.logCertificate(timestamp, treeSize, cert, f)
	if err != nil {
		outcome = outcomeLogCertErr
		return
	}
	// Log frontier
	err = lh.logFrontier(f, gen)
	if err != nil {
		outcome = outcomeLogFrontierErr
		return
	}
	// Commit the changes
	outcome = outcomeSuccess
}
