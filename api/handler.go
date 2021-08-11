package api

import (
	"encoding/json"
	"errors"
	"golang.org/x/xerrors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/hunjixin/grafana-simplejson-mongo/mongodb"
)

type TSQuery struct {
	DB         string
	Collection string
	UserCol    string
	ServiceCol string
	ApiCol     string
	TimeCol    string
	From       time.Time
	To         time.Time
	IntervalMs int
	Type       string
}

func checkRequest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (conf *Config) reqQuery(w http.ResponseWriter, r *http.Request) {
	var result TimeSeriesQuery
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	sp := mongodb.NewSession(conf.MongoHost)
	var q TSQuery
	err := q.parseRangeRaw(result.RangeRaw.From, result.RangeRaw.To)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q.IntervalMs = result.IntervalMs
	var resbytes []byte
	resbytes = append(resbytes, []byte("[")...)
	for _, v := range result.Targets {
		q.Type = v.Type
		err := q.parseTarget(v.Target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if q.Type == "table" {
			log.Println("Table Query")
			resp := TableResponse{Type: v.Type}
			keys, rows, err := sp.GetTableData(q.DB, q.Collection, q.UserCol, q.ServiceCol, q.TimeCol, q.From, q.To)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, v := range keys {
				var column TableColumn
				column.Text = v[0]
				column.Type = v[1]
				resp.Columns = append(resp.Columns, column)
			}
			resp.Rows = rows
			bytes, err := json.Marshal(resp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resbytes = append(resbytes, bytes...)
			resbytes = append(resbytes, []byte(",")...)
		} else if q.Type == "timeserie" {
			log.Println("Time Series Query")
			resp := TimeSeriesResponse{Target: v.Target}
			resp.DataPoint, err = sp.GetTimeSeriesData(q.DB, q.Collection, q.UserCol, q.ServiceCol, q.ApiCol, q.TimeCol, q.From, q.To, q.IntervalMs)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			bytes, err := json.Marshal(resp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resbytes = append(resbytes, bytes...)
			resbytes = append(resbytes, []byte(",")...)
		}
	}
	resbytes = resbytes[:len(resbytes)-1]
	resbytes = append(resbytes, []byte("]")...)
	w.Write(resbytes)
}

var (
	ERRFormat = errors.New("Time format does not match")
)

func (q *TSQuery) parseTarget(target string) error {
	res := strings.Split(target, ".")
	if q.Type == "timeserie" && len(res) < 3 {
		return ERRFormat
	} else if q.Type == "table" && len(res) < 3 { //user/service/time
		return ERRFormat
	}
	q.DB = res[0]
	q.Collection = res[1]
	if q.Type == "timeserie" {
		columns := TimeSeriesColumnRegexp(res[2])
		if len(columns) != 4 {
			return xerrors.New("colum not match")
		}
		q.UserCol = strings.Trim(columns[0], " \t")
		q.ServiceCol = strings.Trim(columns[1], " \t")
		q.ApiCol = strings.Trim(columns[2], " \t")
		q.TimeCol = strings.Trim(columns[3], " \t")
	} else {
		columns := TimeSeriesColumnRegexp(res[2])
		if len(columns) != 3 {
			return xerrors.New("colum not match")
		}
		q.UserCol = strings.Trim(columns[0], " \t")
		q.ServiceCol = strings.Trim(columns[1], " \t")
		q.TimeCol = strings.Trim(columns[2], " \t")
	}
	return nil
}

func (q *TSQuery) parseRangeRaw(from string, to string) error {
	var err error
	if boolRegexp(from, ToNow) {
		q.From, q.To, err = parseToNow(from, to)
		if err != nil {
			return err
		}
	} else if boolRegexp(from, PerNow) {
		q.From, q.To, err = parsePerNow(from, to)
		if err != nil {
			return err
		}
	} else if boolRegexp(from, PerToNow) {
		q.From, q.To, err = parsePerToNow(from, to)
		if err != nil {
			return err
		}
	} else if strings.Contains(from, "Z") {
		q.From, q.To, err = parseISODate(from, to)
		if err != nil {
			return err
		}
	} else {
		return ERRRangeFromat
	}
	return nil
}
