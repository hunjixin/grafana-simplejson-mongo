package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"strconv"
	"time"
)

func (sp *SessionProvider) GetTableData(dbname string, collection string, userCol, serviceCol string, timecol string, from time.Time, to time.Time) ([][]string, [][]interface{}, error) {
	c := sp.Session.Database(dbname).Collection(collection)
	ctx := context.Background()
	sr := c.FindOne(ctx, bson.M{})
	err := sr.Err()
	if err != nil {
		return nil, nil, err
	}
	var judge bson.M
	err = sr.Decode(&judge)
	if err != nil {
		return nil, nil, err
	}

	timecolType := reflect.TypeOf(judge[timecol]).Kind()

	match := bson.M{}
	if timecol != "" {
		var trange bson.M
		switch timecolType {
		case reflect.String:
			trange = bson.M{"$gte": from.Format("20060102150405"), "$lte": to.Format("20060102150405")}
		case reflect.Int, reflect.Float64:
			intFrom, _ := strconv.Atoi(from.Format("20060102150405"))
			intTo, _ := strconv.Atoi(to.Format("20060102150405"))
			trange = bson.M{"$gte": intFrom, "$lte": intTo}
		default:
			trange = bson.M{"$gte": from, "$lte": to}
		}

		match[timecol] = trange
	}
	if userCol != "*" {
		match["name"] = primitive.Regex{Pattern: userCol, Options: "i"}
	}
	if serviceCol != "*" {
		match["service"] = primitive.Regex{Pattern: serviceCol, Options: "i"}
	}

	pipeline := []bson.M{{"$match": match}}

	pipeline = append(pipeline, bson.M{
		"$group": bson.M{
			"_id":   "$method",
			"value": bson.M{"$sum": 1.0},
		},
	})
	pipeline = append(pipeline, bson.M{
		"$sort": bson.M{
			"value": -1.0,
		},
	})

	cur, err := c.Aggregate(ctx, pipeline, options.Aggregate().SetMaxAwaitTime(time.Minute).SetBatchSize(500))
	if err != nil {
		return nil, nil, err
	}

	var results []bson.M
	err = cur.All(ctx, &results)
	if err != nil {
		return nil, nil, err
	}

	var res [][]interface{}
	for _, v := range results {
		array := make([]interface{}, 2)
		array[0] = convertString(v["_id"])
		array[1] = convertFloat(v["value"])
		res = append(res, array)
	}

	return [][]string{{"api", "string"}, {"value", "number"}}, res, nil
}

func defineType(v interface{}) string {
	var ret string
	switch v.(type) {
	case int:
		ret = "number"
	case time.Time:
		ret = "time"
	case primitive.ObjectID:
		ret = "string"
	case string:
		ret = "string"
	}
	return ret
}

func convertString(v interface{}) interface{} {
	var ret interface{}
	switch v.(type) {
	case int:
		ret = strconv.Itoa(v.(int))
	case time.Time:
		ret = v.(time.Time)
	case primitive.ObjectID:
		ret = v.(primitive.ObjectID).String()
	case string:
		ret = v.(string)
	case float64:
		ret = fmt.Sprintf("%f", v.(float64))
	}
	return ret
}
