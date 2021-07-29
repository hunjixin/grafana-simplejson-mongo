SHELL=/usr/bin/env bash

build:
	rm -rf grafana-simplejson-mongo
	go build -o grafana-simplejson-mongo main.go