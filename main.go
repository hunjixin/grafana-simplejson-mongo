package main

import (
	"flag"
	"log"

	"github.com/hunjixin/grafana-simplejson-mongo/api"
)

func main() {
	var port int
	var mongoHost string
	flag.StringVar(&mongoHost, "mongodb", "mongodb://localhost", "mongodb host")
	flag.IntVar(&port, "port", 8082, "server port")
	flag.Parse()
	log.Printf("port: %d, mongodb host: %s\n", port, mongoHost)
	conf := api.Config{
		Port:      port,
		MongoHost: mongoHost,
	}
	errs := make(chan error, 2)
	api.StartHTTPServer(conf, errs)
	log.Println("server start")
	for {
		err := <-errs
		log.Println(err)
	}
}
