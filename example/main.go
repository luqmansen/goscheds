package example

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/luqmansen/goscheds"
	"time"
)

func initRedis() *redis.Client {
	return redis.NewClient(
		&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       1,
		})
}

func main() {

	const job_name = "your_job_name"
	ctx := context.Background()

	// worker and scheduler SHOULD use different redis connection
	// because some worker operation will be blocking
	scheduler := goscheds.NewRedisScheduler("namespace", initRedis())

	// this will start the scheduler
	// it will be blocking, should run on goroutine
	go scheduler.StartScheduler(ctx)

	job := &goscheds.Job{
		JobName:   job_name,
		ExecuteAt: time.Now().Add(24 * time.Hour),
		Args: map[string]interface{}{
			"args": "can be anything",
		},
	}

	// will be executed as time in job.ExecuteAt
	scheduler.PushScheduled(ctx, job)

	// job.ExecuteAt will be ignored, will be executed by worker immediately
	scheduler.Push(ctx, job)

	// job name here should be equal with your registered handler
	worker := goscheds.NewRedisWorker(job_name, initRedis())

	// function handler should satisfy goscheds.HandlerFunc
	worker.RegisterHandler(job_name, func(job *goscheds.Job) error {
		fmt.Println(job.Id, " executed")
		return nil
	})

	worker.ProcessJob(ctx)
}
