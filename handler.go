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

var (
	frontierName     = "f4"
	certificatesName = "c4"
	// store frontier in database or in local memory
	localFrontier = true
	frontierCache frontier
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
	Mutex  *sync.Mutex
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
//    This is the master table and the only one that truly needs to remain
//    consistent. Everytime we get a new certificate, we append a record to the
//    table by inserting a record at the given treeSize. This will remain
//    consistent because aerospike provides record-level atomicity and will
//    prevent updates (versus an insert). As such, we should never have the
//    possibility of trying to add two certificates at the same treeSize.
//
//	  Optimally, there would be a way to get the latest treeSize out of this
//	  table, however, this doesn't seem possible without an expensive map
//	  reduce. So, we need to cache this somewhere. Note, that it's "ok" if the
//	  cacheTable is behind--- it can just never be head of the certificates
//	  table. The worst case scenario if the cache table is behind is that the
//	  write to certificates table fails, we realize that something is astray,
//	  run the map reduce, and repopulate the cache table. If the cache table
//	  somehow got ahead of the certificates table, we're screwed, but it seems
//	  like this is easily preventable by just always writing to the
//	  certificates table and making sure that that write is persisted before
//	  ever writing to the cache table.

// 2. frontier{id int [key](always 1) -> locked bool, lockedAt datetime, frontier []byte}
//
//    This table contains a cache of the latest frontier and treeSize. If there's a crash
//    between a write to certificates and a write to this table, this table *can* enter an
//    inconsistent state. In this situation, we have to do a map reduce over certificates
//    to recalculate it. It's easy to determine whether this has happened, because it will
//    result in a failed write to the certificates table.
//
//    This table doesn't need to exist in the database persay, it could also be in a faster
//    shared in memory datastore, e.g., memcached or redis

func (lh *LogHandler) readFrontier() (frontier, uint32, error) {
	// set lock bit in frontier table. if cannot set, keep trying using exponential
	// backoff. If we can't get a lock in 10 seconds, something is probably wrong, give up.
	// once lock is achieved, figure out what treeSize slot we can use.
	//fmt.Println("read frontier called")
	if localFrontier {
		return frontierCache, 0, nil
	}
	// otherwise go get it from the database
	key, err := as.NewKey("ctmem", frontierName, 0)
	if err != nil {
		panic(err)
	}
	readPolicy := as.NewPolicy()
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	var rec *as.Record
	for {
		//fmt.Println("in loop")
		rec, err = lh.Client.Get(readPolicy, key)
		if err != nil {
			panic(err)
		}
		//fmt.Println("current state of frontier:", rec)
		if rec != nil && rec.Bins["locked"].(int) != 0 {
			//fmt.Println("locked")
			continue
		}
		// woo! we're not locked! lock database.
		//fmt.Println("got frontier:", rec)
		if rec != nil {
			//fmt.Println("existing frontier")
			writePolicy.Generation = rec.Generation
		} else {
			//fmt.Println("no existing frontier")
			writePolicy.Generation = 0
		}
		bin := as.NewBin("locked", 1)
		err := lh.Client.PutBins(writePolicy, key, bin)
		if err != nil {
			continue
		}
		break
	}
	f := frontier{}
	//fmt.Println("got lock", f)
	if rec != nil {
		//fmt.Println("will try to decode")
		err = f.Unmarshal(rec.Bins["frontier"].([]byte))
		if err != nil {
			panic("could not decode frontier")
		}
		//fmt.Println("got the following frontier", f)
		return f, rec.Generation, err
	}
	return f, 0, err
}

func (lh *LogHandler) logCertificate(timestamp int64, treeSize int64, cert []byte, f frontier) error {
	//fmt.Println("going to log certificate at", treeSize)
	writePolicy := as.NewWritePolicy(0, 0)
	// make sure we don't ever overwrite an existing record due to a lock failure.
	// this is an append only data structure.
	writePolicy.RecordExistsAction = as.CREATE_ONLY
	certBin := as.NewBin("certificate", cert)
	frontBin := as.NewBin("frontier", f.Marshal())
	key, err := as.NewKey("ct", certificatesName, treeSize)
	err = lh.Client.PutBins(writePolicy, key, certBin, frontBin)
	return err
}

func (lh *LogHandler) logFrontier(f frontier, generation uint32) error {
	if localFrontier {
		frontierCache = f
	}
	writePolicy := as.NewWritePolicy(0, 0)
	//writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	//writePolicy.Generation = generation
	//fmt.Println("going to log following frontier", f)
	key, err := as.NewKey("ctmem", frontierName, 0)
	frontBin := as.NewBin("frontier", f.Marshal())
	lockBin := as.NewBin("locked", 0)
	err = lh.Client.PutBins(writePolicy, key, frontBin, lockBin)
	//fmt.Println("released lock")
	return err
}

func (lh *LogHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	//fmt.Println("start")
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
	lh.Mutex.Lock()
	defer lh.Mutex.Unlock()
	// Await access to the DB (local per-process lock only)
	//gotLock := make(chan bool, 1)
	//cancelled := make(chan bool, 1)
	//go func() {
	//	lh.dbLock.Lock()
	//	gotLock <- true
	//	if <-cancelled {
	//		lh.dbLock.Unlock()
	//	}
	//}()

	//select {
	//case <-gotLock:
	//case <-time.After(500 * time.Millisecond):
	//	cancelled <- true
	//	outcome = outcomeDBLockTimeout
	//	return
	//}
	//defer lh.dbLock.Unlock()

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
	//fmt.Println("read frontier")
	// Update the frontier with this certificate
	enterUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	//fmt.Println("old tree size", f.Size())
	f.Add(cert)
	treeSize = int64(f.Size())
	//fmt.Println("new tree size", treeSize)
	exitUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	updateTime.Observe(exitUpdate - enterUpdate)
	// Log the certificate
	timestamp := time.Now().Unix()
	err = lh.logCertificate(timestamp, treeSize, cert, f)
	//fmt.Println("logged cert")
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
	//fmt.Println("logged frontier")
	// Commit the changes
	outcome = outcomeSuccess
}
