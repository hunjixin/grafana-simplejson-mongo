package mongodb

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"reflect"
	"sort"
	"strconv"
	"time"
)

func (sp *SessionProvider) GetTimeSeriesData(dbname string, collection string, userCol, serviceCol, apiCol, timeCol string, from time.Time, to time.Time, intervalMs int) ([][]float64, error) {
	ctx := context.TODO()
	var res [][]float64
	c := sp.Session.Database(dbname).Collection(collection)
	sr := c.FindOne(ctx, bson.M{})
	err := sr.Err()
	if err != nil {
		return res, err
	}
	var judge bson.M
	err = sr.Decode(&judge)
	if err != nil {
		return res, err
	}

	timecolType := reflect.TypeOf(judge[timeCol]).Kind()
	pipeline := BuildTimeSeriesPipe(userCol, serviceCol, apiCol, timeCol, from, to, intervalMs, timecolType)
	cur, err := c.Aggregate(ctx, pipeline)
	if err != nil {
		return res, err
	}
	var results []bson.M
	err = cur.All(ctx, &results)
	if err != nil {
		return res, err
	}

	for _, v := range results {
		array := make([]float64, 2)
		var date time.Time
		switch timecolType {
		case reflect.Int, reflect.Float64:
			date, err = parseInttoDate(int(v["_id"].(float64)))
			if err != nil {
				return res, err
			}
		default:
			date, err = parseIdtoDate(v, intervalMs)
			if err == ERRNilDataPoint {
				log.Println("Contain invalid data")
				continue
			} else if err != nil {
				return res, err
			}
		}
		array[0] = convertFloat(v["value"])
		array[1] = convertFloat(date)
		res = append(res, array)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i][1] < res[j][1]
	})
	return res, nil
}

func BuildTimeSeriesPipe(userCol, serviceCol, apiCol, timecol string, from time.Time, to time.Time, intervalMs int, timecolType reflect.Kind) []bson.M {
	var trange bson.M
	switch timecolType {
	case reflect.String:
		trange = bson.M{timecol: bson.M{"$gte": from.Format("20060102150405"), "$lte": to.Format("20060102150405")}}
	case reflect.Int, reflect.Float64:
		intFrom, _ := strconv.Atoi(from.Format("20060102150405"))
		intTo, _ := strconv.Atoi(to.Format("20060102150405"))
		trange = bson.M{timecol: bson.M{"$gte": intFrom, "$lte": intTo}}
	default:
		trange = bson.M{timecol: bson.M{"$gte": from, "$lte": to}}
	}

	pipeline := []bson.M{{"$match": bson.M{"$and": []interface{}{trange}}}} //time

	match := bson.D{}
	if userCol != "*" {
		match = append(match, bson.E{"name", primitive.Regex{Pattern: "/" + userCol + "/", Options: "i"}})
	}
	if serviceCol != "*" {
		match = append(match, bson.E{"service", primitive.Regex{Pattern: "/" + serviceCol + "/", Options: "i"}})
	}
	if apiCol != "*" {
		match = append(match, bson.E{Key: "method", Value: primitive.Regex{Pattern: "/" + apiCol + "/", Options: "i"}})
	}

	pipeline = append(pipeline, bson.M{"$match": match}) //user

	pipeline = append(pipeline, bson.M{
		"$group": bson.M{
			"_id":   buildTimeBson(timecol, intervalMs, timecolType),
			"value": bson.M{"$sum": 1},
		},
	})
	return pipeline
}

//{ "$sort": bson.M{ "_id": 1}},
func buildTimeBson(timecol string, intervalMs int, timecolType reflect.Kind) bson.M {
	var ret bson.M
	var TimeCol interface{}
	switch timecolType {
	case reflect.String:
		TimeCol = bson.M{"$dateFromString": bson.M{"dateString": "$" + timecol}}
	case reflect.Int, reflect.Float64:
		interval := intervalMs / 1000
		if interval < 1 {
			interval = 1
		}
		return bson.M{"$subtract": []interface{}{"$" + timecol, bson.M{"$mod": []interface{}{"$" + timecol, interval}}}}
	default:
		TimeCol = "$" + timecol
	}
	if 86400000 <= intervalMs && intervalMs < 2629800000 {
		uni := "$day"
		return bson.M{
			"year": bson.M{
				"$year": TimeCol,
			},
			"month": bson.M{
				"$month": TimeCol,
			},
			"day": buildInterval(timecol, intervalMs, uni, 86400000, timecolType),
		}
	} else if 3600000 <= intervalMs && intervalMs < 86400000 {
		uni := "$hour"
		return bson.M{
			"year": bson.M{
				"$year": TimeCol,
			},
			"month": bson.M{
				"$month": TimeCol,
			},
			"day": bson.M{
				"$dayOfMonth": TimeCol,
			},
			"hour": buildInterval(timecol, intervalMs, uni, 3600000, timecolType),
		}
	} else if 60000 <= intervalMs && intervalMs < 3600000 {
		uni := "$minute"
		return bson.M{
			"year": bson.M{
				"$year": TimeCol,
			},
			"month": bson.M{
				"$month": TimeCol,
			},
			"day": bson.M{
				"$dayOfMonth": TimeCol,
			},
			"hour": bson.M{
				"$hour": TimeCol,
			},
			"minute": buildInterval(timecol, intervalMs, uni, 60000, timecolType),
		}
	} else {
		uni := "$second"
		return bson.M{
			"year": bson.M{
				"$year": TimeCol,
			},
			"month": bson.M{
				"$month": TimeCol,
			},
			"day": bson.M{
				"$dayOfMonth": TimeCol,
			},
			"hour": bson.M{
				"$hour": TimeCol,
			},
			"minute": bson.M{
				"$minute": TimeCol,
			},
			"interval": buildInterval(timecol, intervalMs, uni, 1000, timecolType),
		}
	}
	return ret
}

func buildInterval(timecol string, intervalMs int, uni string, ms int, timecolType reflect.Kind) bson.M {
	type list []interface{}
	interval := intervalMs / ms
	if interval < 1 {
		interval = 1
	}
	var uniTime bson.M
	switch timecolType {
	case reflect.String:
		uniTime = bson.M{uni: bson.M{"$dateFromString": bson.M{"dateString": "$" + timecol}}}
	default:
		uniTime = bson.M{uni: "$" + timecol}
	}
	mod := list{uniTime, interval}
	sub := list{uniTime, bson.M{"$mod": mod}}
	return bson.M{"$subtract": sub}
}

func parseIdtoDate(v bson.M, intervalMs int) (time.Time, error) {
	var year, month, day, hour, minute, second int32
	if v["_id"].(bson.M)["year"] == nil {
		log.Println("1")
		return time.Time{}, ERRNilDataPoint
	}
	year = v["_id"].(bson.M)["year"].(int32)

	if v["_id"].(bson.M)["month"] == nil {
		log.Println("2")
		return time.Time{}, ERRNilDataPoint
	}
	month = v["_id"].(bson.M)["month"].(int32)

	if intervalMs < 2629800000 {
		if v["_id"].(bson.M)["day"] == nil {
			log.Println("3")
			return time.Time{}, ERRNilDataPoint
		}
		day = v["_id"].(bson.M)["day"].(int32)
		if intervalMs >= 86400000 {
			goto fin
		}
	}
	if intervalMs < 86400000 { //1 day
		if v["_id"].(bson.M)["hour"] == nil {
			log.Println("4")
			return time.Time{}, ERRNilDataPoint
		}
		hour = v["_id"].(bson.M)["hour"].(int32)
		if intervalMs >= 3600000 { //1h
			goto fin
		}
	}
	if intervalMs < 3600000 {
		if v["_id"].(bson.M)["minute"] == nil {
			log.Println("5")
			return time.Time{}, ERRNilDataPoint
		}

		minute = v["_id"].(bson.M)["minute"].(int32)
		if intervalMs >= 60000 {
			goto fin
		}
	}
	if intervalMs < 60000 {
		if v["_id"].(bson.M)["interval"] == nil {
			log.Println("6")
			return time.Time{}, ERRNilDataPoint
		}
		second = v["_id"].(bson.M)["interval"].(int32)
	}

fin:
	return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), 0, time.UTC), nil
}

func parseInttoDate(date int) (time.Time, error) {
	return time.Date(date/10000000000, time.Month((date/100000000)%100), (date/1000000)%100, (date/10000)%100, (date/100)%100, date%100, 0, time.UTC), nil
}
