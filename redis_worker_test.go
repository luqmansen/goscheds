package goscheds

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	filename "github.com/keepeye/logrus-filename"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	filenameHook := filename.NewHook()
	filenameHook.Field = "src"
	log.AddHook(filenameHook)
	log.SetLevel(log.DebugLevel)
}

func TestNewRedisWorker(t *testing.T) {

	t.Run("execute scheduled task", func(t *testing.T) {

		schedulerClient := initRedis()
		workerClient := initRedis()
		namespace := uuid.NewString()
		scheduler := NewRedisScheduler(namespace, schedulerClient)
		worker := NewRedisWorker(namespace, workerClient)
		ctx := context.Background()
		schedulerClient.FlushDB(ctx)

		queueName := "test_func"

		jobPayload := &Job{
			JobName:   queueName,
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": uuid.NewString()},
		}

		executed := make(chan bool, 1)
		worker.RegisterHandler(queueName, func(job *Job) error {
			executed <- true
			return nil
		})

		worker.ProcessJob(ctx)
		assert.NoError(t, scheduler.PushScheduled(ctx, jobPayload))
		time.Sleep(time.Second)
		assert.True(t, <-executed)

	})

	t.Run("failed job retried", func(t *testing.T) {

		schedulerClient := initRedis()
		workerClient := initRedis()
		namespace := uuid.NewString()
		scheduler := NewRedisScheduler(namespace, schedulerClient)
		worker := NewRedisWorker(namespace, workerClient)
		ctx := context.Background()
		schedulerClient.FlushDB(ctx)

		queueName := "test_func"

		jobPayload := &Job{
			JobName:   queueName,
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{},
		}

		executed := make(chan bool, 1)
		counter := 0

		worker.RegisterHandler(queueName, func(job *Job) error {
			if counter != 2 {
				counter += 1
				return fmt.Errorf("some error")
			}
			executed <- true
			return nil
		})

		worker.ProcessJob(ctx)
		assert.NoError(t, scheduler.PushScheduled(ctx, jobPayload))
		time.Sleep(1 * time.Second)
		assert.True(t, <-executed)
	})
}
