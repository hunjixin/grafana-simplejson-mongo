package api

import (
	"encoding/json"
	"log"
	"net/http"
)

func (conf *Config) reqSearch(w http.ResponseWriter, r *http.Request) {
	log.Println("Search Query")
	/*fake*/
	var result SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	//{用户，服务,api,time}
	bytes, err := json.Marshal([]string{
		"traces.call",              //no work
		"traces.call.{*,*,*,time}", //某个用户 api
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(bytes)
}
