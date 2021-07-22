package main

import (
	"log"

	"github.com/hunjixin/grafana-simplejson-mongo/api"
)

func main() {
	conf := api.Config{
		Port:      8080,
		MongoHost: "mongodb://8.130.164.80:27017",
	}
	errs := make(chan error, 2)
	api.StartHTTPServer(conf, errs)
	log.Println("start")
	for {
		err := <-errs
		log.Println(err)
	}
}
