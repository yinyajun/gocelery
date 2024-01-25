package main

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yinyajun/gocelery"
)

var Client *gocelery.CeleryClient

// celery -A app worker -l debug -Q q1 --without-heartbeat --without-mingle
func init() {
	brokerPool := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	backendPool := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// initialize celery client
	broker := gocelery.NewRedisBroker(brokerPool)
	backend := gocelery.NewRedisBackend(backendPool)
	cli, _ := gocelery.NewCeleryClient(broker, backend)
	Client = cli

}

func sendTask() {
	// prepare arguments
	taskName := "mq.tasks.image_generate"

	scheme := "anythingv5_gufeng"
	prompt := "a girl"
	negPrompt := "bad quality"
	seed := 12

	// run task
	asyncResult1, err := Client.ApplyAsync(
		taskName,
		nil,
		gocelery.WithKwArgs(map[string]interface{}{
			"scheme_name":      scheme,
			"prompt":           prompt,
			"neg_prompt":       negPrompt,
			"width":            768,
			"height":           1024,
			"seed":             seed,
			"image_num":        1,
			"watermark":        "432",
			"secret_watermark": "",
		}),
		gocelery.WithQueue("sd_text2img_anime1"),
		gocelery.WithExpires(15*time.Second),
	)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(asyncResult1.TaskID)

	// get results from backend with timeout
	res, err := asyncResult1.Get(2 * time.Second)
	if err != nil {
		fmt.Println(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))
	fmt.Println(asyncResult1.TaskID)
}

func main() {
	for i := 0; i < 1; i++ {
		var wg sync.WaitGroup

		for j := 0; j < 1; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendTask()
			}()
		}
		wg.Wait()
	}
}
