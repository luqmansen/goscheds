package goscheds

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v9"
	log "github.com/sirupsen/logrus"
)

type RedisScheduler struct {
	namespace string
	client    *redis.Client
	locker    *redislock.Client
	*sync.Mutex
}

var _ SchedulerService = (*RedisScheduler)(nil)

func NewRedisScheduler(namespace string, client *redis.Client) *RedisScheduler {

	redisLocker := redislock.New(client)

	return &RedisScheduler{
		namespace: namespace,
		locker:    redisLocker,
		client:    client,
		Mutex:     &sync.Mutex{},
	}
}

//RegisterHandler will register handler to the

const (
	scheduledKeyName = "scheduled_task"
)

//Push will push a job right away to work queue, ExecuteAt field will be ignored
func (r *RedisScheduler) Push(ctx context.Context, job *Job) error {
	job.Id = uuid.NewString()

	return r.addItemToWorkQueue(ctx, job)
}

//PushScheduled will push job to a queue. A handler for pushed must
//be registered before pushing job to the queue
func (r *RedisScheduler) PushScheduled(ctx context.Context, job *Job) error {
	job.Id = uuid.NewString()

	log.Debugf("pushing job %s args %s", job.JobName, job.Args)

	jobB, err := job.marshal()
	if err != nil {
		log.Error(err)
		return err
	}

	if job.isReadyToExecute() {
		lockKey := fmt.Sprintf("%s:LOCK:%s", r.namespace, scheduledKeyName)
		locker, err := r.locker.Obtain(ctx, lockKey, 100*time.Millisecond, nil)
		if err != nil {
			log.Error(err)
			return err
		}

		pushOps := func() error {
			if err := r.addItemToWorkQueue(ctx, job); err != nil {
				log.Error(err)
			}
			return err
		}

		err = backoff.Retry(pushOps, backoff.NewConstantBackOff(time.Millisecond))
		if err != nil {
			locker.Release(ctx)
			log.Error(err)
			return err
		}

		locker.Release(ctx)
		return nil
	}

	key := fmt.Sprintf("%s:%s", r.namespace, scheduledKeyName)

	secondsFromNow := job.ExecuteAt.Sub(time.Now()).Seconds()
	score := float64(time.Now().Unix()) + secondsFromNow

	return r.client.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: jobB,
	}).Err()
}

//StartScheduler will start the scheduling process. This is blocking,
//thus need to be running on its own goroutine
func (r *RedisScheduler) StartScheduler(ctx context.Context) {
	key := fmt.Sprintf("%s:LOCK:%s", r.namespace, scheduledKeyName)

	for {
		time.Sleep(10 * time.Second)
		locker, err := r.locker.Obtain(ctx, key, 100*time.Millisecond, nil)
		if err != nil {
			log.Error(err)
			continue
		}

		res, err := r.getTopList(ctx)
		// no item in the list
		if res == nil && err == nil {
			locker.Release(ctx)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			log.Error(err)
			locker.Release(ctx)
			continue
		}

		// check if the task date is match the current date
		if res.isReadyToExecute() {
			if err := r.deleteFromList(ctx, res); err != nil {
				locker.Release(ctx)
				log.Error(err)
				continue
			}

			pushOps := func() error {
				if err := r.addItemToWorkQueue(ctx, res); err != nil {
					log.Error(err)
				}
				return err
			}

			err = backoff.Retry(pushOps, backoff.NewConstantBackOff(time.Millisecond))
			if err != nil {
				locker.Release(ctx)
				log.Error(err)
			}
		} else {
			locker.Release(ctx)
		}

		locker.Release(ctx)
	}
}

func (r *RedisScheduler) getTopList(ctx context.Context) (*Job, error) {
	key := fmt.Sprintf("%s:%s", r.namespace, scheduledKeyName)

	res, err := r.client.ZRange(ctx, key, 0, 0).Result()
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	var job *Job
	err = json.Unmarshal([]byte(res[0]), &job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (r *RedisScheduler) deleteFromList(ctx context.Context, job *Job) error {
	key := fmt.Sprintf("%s:%s", r.namespace, scheduledKeyName)
	// ignore the error, bcs this must be a valid struct
	jobB, _ := job.marshal()

	return r.client.ZRem(ctx, key, jobB).Err()
}

func (r *RedisScheduler) addItemToWorkQueue(ctx context.Context, job *Job) error {
	key := fmt.Sprintf("%s:%s", r.namespace, job.JobName)

	// ignore the error, bcs this must be a valid struct
	val, _ := job.marshal()

	return r.client.RPush(ctx, key, val).Err()
}
