package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
        "sync"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/bifurcation/loggerhead"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

/*

Test invocation:

> go run main.go -conn "user=rbarnes dbname=rbarnes sslmode=disable"

Test query:

-----
POST /ct/v1/add-chain HTTP/1.1
Host: localhost:8080
Content-Type: text/json
Content-Length: 58

{"chain":["8TpFNrV+YbVkOX6VRjDoxKGb32DNgBo0nPNgOvivsnA="]}
-----

*/

func main() {
	var conn, port string
	flag.StringVar(&conn, "conn", "", "Connection string")
	flag.StringVar(&port, "port", "8080", "Port")
	flag.Parse()

	client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to Aerospike server: %v", err)
	}
	var m sync.Mutex

	handler := &loggerhead.LogHandler{Client: client, Mutex: &m}

	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/ct/v1/add-chain", handler)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
