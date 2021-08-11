package main

import (
	"flag"
	"log"

	"github.com/hunjixin/grafana-simplejson-mongo/api"
)

func main() {
	var port int
	var mongoHost string
	flag.StringVar(&mongoHost, "mongodb", "mongodb://47.251.4.254:27017", "mongodb host")
	//flag.StringVar(&mongoHost, "mongodb", "mongodb://8.130.160.22:27017", "mongodb host")
	flag.IntVar(&port, "port", 8080, "server port")
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
