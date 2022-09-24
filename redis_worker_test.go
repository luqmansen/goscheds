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

	schedulerClient := initRedis()
	workerClient := initRedis()
	scheduler := NewRedisScheduler("test", schedulerClient)
	worker := NewRedisWorker("test", workerClient)
	ctx := context.Background()
	schedulerClient.FlushDB(ctx)

	t.Run("execute scheduled task", func(t *testing.T) {
		queueName := "test_func"

		jobPayload := &ScheduledJob{
			JobName:   queueName,
			Id:        uuid.NewString(),
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": uuid.NewString()},
		}

		executed := false
		worker.RegisterHandler(queueName, func(job *ScheduledJob) error {
			assert.Equal(t, jobPayload.Id, job.Id)
			executed = true
			return nil
		})

		worker.ProcessJob(ctx)
		assert.NoError(t, scheduler.PushScheduled(ctx, jobPayload))
		time.Sleep(time.Second)
		assert.True(t, executed)

	})

	t.Run("failed job retried", func(t *testing.T) {
		queueName := "test_func"

		jobPayload := &ScheduledJob{
			JobName:   queueName,
			Id:        uuid.NewString(),
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": uuid.NewString()},
		}

		count := 0
		executed := false
		worker.RegisterHandler(queueName, func(job *ScheduledJob) error {
			assert.Equal(t, jobPayload.Id, job.Id)
			if count != 2 {
				count += 1
				return fmt.Errorf("some error")
			}
			executed = true
			return nil
		})

		worker.ProcessJob(ctx)
		assert.NoError(t, scheduler.PushScheduled(ctx, jobPayload))
		time.Sleep(1 * time.Second)
		assert.True(t, executed)
	})
}
