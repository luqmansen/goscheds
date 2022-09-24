package goscheds

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func initRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       9,
	})
}

func TestRedisScheduler(t *testing.T) {

	client := initRedis()
	scheduler := NewRedisScheduler("test", client)
	ctx := context.Background()
	client.FlushDB(ctx)

	t.Run("isJobReadyToExecute", func(t *testing.T) {
		j := Job{
			ExecuteAt: time.Now(),
		}
		assert.True(t, isJobReadyToExecute(j))

		j = Job{
			ExecuteAt: time.Now().Add(200 * time.Hour),
		}
		assert.False(t, isJobReadyToExecute(j))
	})

	t.Run("success push job for next day", func(t *testing.T) {
		job := &Job{
			JobName:   "test_func",
			ExecuteAt: time.Now().Add(2 * 24 * time.Hour), // two days from now
			Args:      map[string]interface{}{"test": uuid.NewString()},
		}
		assert.NoError(t, scheduler.PushScheduled(ctx, job))
		res, err := scheduler.getTopList(ctx)
		assert.NoError(t, err)

		assert.False(t, isJobReadyToExecute(*res))

		client.FlushDB(ctx)
	})

	t.Run("success push job for today", func(t *testing.T) {

		job := &Job{
			JobName:   "test_func",
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": uuid.NewString()},
		}
		assert.NoError(t, scheduler.PushScheduled(ctx, job))
		res, err := scheduler.getTopList(ctx)
		assert.NoError(t, err)
		// list should be empty, because the job is pushed to the work queue right away
		assert.Nil(t, res)

		client.FlushDB(ctx)
	})

	t.Run("get top of the list item exists", func(t *testing.T) {

		job := Job{
			JobName:   "test_func",
			id:        uuid.NewString(),
			ExecuteAt: time.Now().Add(1 * time.Hour),
			Args:      map[string]interface{}{"test": 123},
		}
		job2 := Job{
			JobName:   "test_func",
			id:        uuid.NewString(),
			ExecuteAt: time.Now().Add(2 * time.Hour), // the score for this member should be higher than prev job
			Args:      map[string]interface{}{"test": 123},
		}

		assert.NoError(t, scheduler.PushScheduled(ctx, &job))
		assert.NoError(t, scheduler.PushScheduled(ctx, &job2))
		res, err := scheduler.getTopList(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, job.id, res.id)
		client.FlushDB(ctx)
	})

	t.Run("get top of the list, no item ", func(t *testing.T) {
		res, err := scheduler.getTopList(ctx)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("remove item from list", func(t *testing.T) {

		job := &Job{
			JobName:   "test_func",
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": 123},
		}

		assert.NoError(t, scheduler.PushScheduled(ctx, job))
		assert.NoError(t, scheduler.deleteFromList(ctx, job))

		res, err := scheduler.getTopList(ctx)
		assert.NoError(t, err)
		assert.Nil(t, res)

		client.FlushDB(ctx)
	})

	t.Run("add item to work queue", func(t *testing.T) {
		job := &Job{
			JobName:   "test_func",
			ExecuteAt: time.Now(),
			Args:      map[string]interface{}{"test": 123},
		}

		assert.NoError(t, scheduler.addItemToWorkQueue(ctx, job))
		client.FlushDB(ctx)
	})

}
