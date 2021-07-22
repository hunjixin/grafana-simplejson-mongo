package mongodb

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type SessionProvider struct {
	Session *mongo.Client
}

func NewSession(host string) SessionProvider {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(host))
	if err != nil {
		panic(err)
	}
	return SessionProvider{
		client,
	}
}

func convertFloat(v interface{}) float64 {
	var r float64
	switch v.(type) {
	case int:
		r = float64(v.(int))
	case int32:
		r = float64(v.(int32))
	case int64:
		r = float64(v.(int64))
	case float64:
		r = v.(float64)
	case time.Time:
		r = float64(v.(time.Time).UnixNano() / int64(time.Millisecond))
	}
	return r
}

var (
	ERRNilDataPoint = errors.New("NilDataPoint")
)

func parseDate(v bson.M, num int) (time.Time, error) {
	var year, month, day, hour, minute, second, milisec int
	log.Println(v)
	for i := 0; i <= num; i++ {
		switch i {
		case 0:
			if v["_id"].(bson.M)["year"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			year = v["_id"].(bson.M)["year"].(int)
		case 1:
			if v["_id"].(bson.M)["month"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			month = v["_id"].(bson.M)["month"].(int)
		case 2:
			if v["_id"].(bson.M)["day"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			day = v["_id"].(bson.M)["day"].(int)
		case 3:
			if v["_id"].(bson.M)["hour"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			hour = v["_id"].(bson.M)["hour"].(int)
		case 4:
			if v["_id"].(bson.M)["minute"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			minute = v["_id"].(bson.M)["minute"].(int)
		case 5:
			if v["_id"].(bson.M)["second"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			second = v["_id"].(bson.M)["second"].(int)
		case 6:
			if v["_id"].(bson.M)["milisecond"] == nil {
				return time.Time{}, ERRNilDataPoint
			}
			milisec = v["_id"].(bson.M)["milisecond"].(int)
		}
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, milisec, time.UTC), nil
}
