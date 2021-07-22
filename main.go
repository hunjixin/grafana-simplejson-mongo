package main

import (
	"log"

	"github.com/hunjixin/grafana-simplejson-mongo/api"
)

func main() {
	conf := api.Config{
		Port:      8080,
		MongoHost: "mongodb://127.0.0.1:27017",
	}
	errs := make(chan error, 2)
	api.StartHTTPServer(conf, errs)
	log.Println("start")
	for {
		err := <-errs
		log.Println(err)
	}
}
