package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"strconv"
	"time"
)

func (sp *SessionProvider) GetTableData(dbname string, collection string, timecol string, from time.Time, to time.Time) ([][]string, [][]interface{}, error) {
	var keys [][]string
	var rows [][]interface{}
	c := sp.Session.Database(dbname).Collection(collection)

	var find bson.M
	if timecol != "" {
		find = bson.M{
			timecol: bson.M{
				"$gt": from,
				"$lt": to,
			},
		}
	} else {
		find = nil
	}

	var results []bson.M
	ctx := context.TODO()
	cur, err := c.Find(ctx, find)
	if err != nil {
		log.Println(err)
		return keys, rows, err
	}
	err = cur.All(ctx, &results)
	if err != nil {
		log.Println(err)
		return keys, rows, err
	}

	if len(results) < 1 {
		return keys, rows, nil
	}
	for k, v := range results[0] {
		var key []string
		key = append(key, k)
		key = append(key, defineType(v))
		keys = append(keys, key)
	}

	for i := 0; i < len(results); i++ {
		var row []interface{}
		for _, key := range keys {
			row = append(row, convertString(results[i][key[0]]))
		}
		rows = append(rows, row)
	}
	return keys, rows, nil
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
