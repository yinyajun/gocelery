package main

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yinyajun/gocelery"
)

// Run Celery Worker First!
// celery -A app worker -l debug -Q q1 --without-heartbeat --without-mingle
func main() {

	// create redis connection pool
	redisPool := &redis.Pool{
		MaxIdle:     32,                // maximum number of idle connections in the pool
		MaxActive:   64,                // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 240 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://localhost:6379/0")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	// initialize celery client
	broker := gocelery.NewRedisBroker(redisPool)
	backend := gocelery.NewRedisBackend(redisPool)
	broker.QueueName = "q1"
	cli, _ := gocelery.NewCeleryClient(
		broker,
		backend,
	)

	// prepare arguments
	taskName := "sd.tasks.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)
	fmt.Println(argA, argB)

	// run task
	asyncResult, err := cli.Delay(taskName, argA, argB)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err := asyncResult.Get(31 * time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))
}
