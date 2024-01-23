package main

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yinyajun/gocelery"
)

var Client *gocelery.CeleryClient

// celery -A app worker -l debug -Q q1 --without-heartbeat --without-mingle
func init() {
	// create redis connection pool
	redisBrokerPool := &redis.Pool{
		MaxIdle:     16,               // maximum number of idle connections in the pool
		MaxActive:   32,               // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 60 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://:6$vx494k98rOZoAE0J@bj-crs-0m242w8s.sql.tencentcdb.com:29320/0")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	redisBackendPool := &redis.Pool{
		MaxIdle:     32,                // maximum number of idle connections in the pool
		MaxActive:   64,                // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 240 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://:6$vx494k98rOZoAE0J@bj-crs-bhlxfvb0.sql.tencentcdb.com:20123/0")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	// initialize celery client
	broker := gocelery.NewRedisBroker(redisBrokerPool)
	backend := gocelery.NewRedisBackend(redisBackendPool)
	cli, _ := gocelery.NewCeleryClient(broker, backend)
	Client = cli
}

func sendTask() {
	// prepare arguments
	taskName := "mq.tasks.image_generate"

	scheme := "anythingv5_gufeng"
	prompt := "a girl"
	negPrompt := "bad quality"
	width := 768
	height := 1024
	seed := 12
	num := 1
	waterMark := "4234"
	scret := ""

	// run task
	asyncResult1, err := Client.ApplyAsync(
		taskName,
		[]interface{}{scheme, prompt, negPrompt, width, height, seed, num, waterMark, scret},
		gocelery.WithQueue(""),
		gocelery.WithExpires(20*time.Second),
	)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(asyncResult1.TaskID)

	// get results from backend with timeout
	res, err := asyncResult1.Get(20 * time.Second)
	if err != nil {
		fmt.Println(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))
	fmt.Println(asyncResult1.TaskID)
}

func main() {
	//for i := 0; i < 1; i++ {
	//	var wg sync.WaitGroup
	//
	//	for j := 0; j < 3; j++ {
	//		wg.Add(1)
	//		go func() {
	//			defer wg.Done()
	//			sendTask()
	//		}()
	//	}
	//	wg.Wait()
	//}
	sendTask()
}
