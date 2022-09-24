package goscheds

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v9"
	log "github.com/sirupsen/logrus"
)

type RedisWorker struct {
	namespace  string
	client     *redis.Client
	jobHandler map[string]HandlerFunc
	locker     *redislock.Client
	*sync.Mutex
}

var _ WorkerService = (*RedisWorker)(nil)

func NewRedisWorker(namespace string, client *redis.Client) *RedisWorker {

	redisLocker := redislock.New(client)

	return &RedisWorker{
		namespace:  namespace,
		locker:     redisLocker,
		client:     client,
		jobHandler: make(map[string]HandlerFunc),
		Mutex:      &sync.Mutex{},
	}
}

func (r *RedisWorker) RegisterHandler(name string, fn HandlerFunc) {
	r.Lock()
	defer r.Unlock()

	r.jobHandler[name] = fn
}

func (r *RedisWorker) ProcessJob(ctx context.Context) {
	for k, _ := range r.jobHandler {
		go r.processJobForQueue(ctx, k)
	}
}

const onProgressSuffixKey = "on_progress"

func (r *RedisWorker) processJobForQueue(ctx context.Context, queueName string) {
	lockKey := fmt.Sprintf("%s:LOCK:%s", r.namespace, queueName)

	for {
		locker, err := r.locker.Obtain(ctx, lockKey, time.Second, nil)
		if err != nil {
			log.Error(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		queueKey := fmt.Sprintf("%s:%s", r.namespace, queueName)
		res, err := r.client.BLPop(ctx, time.Second, queueKey).Result()
		if err != nil && err != redis.Nil {
			log.Error(err)
			locker.Release(ctx)
			continue
		}

		if len(res) < 2 {
			locker.Release(ctx)
			continue
		}

		var job *ScheduledJob
		_ = json.Unmarshal([]byte(res[1]), &job)

		log.Debugf("processing %s args %s", queueName, job.Args)

		if err := r.addItemToOnProgressQueue(ctx, job); err != nil {
			log.Error(err)
			locker.Release(ctx)
			continue
		}

		locker.Release(ctx)

		fn := r.jobHandler[queueName]

		err = fn(job)
		if err != nil {
			err = r.requeueFailedJob(ctx, job)
			if err != nil {
				log.Error(err)
				locker.Release(ctx)
				continue
			}
		} else {
			err = r.deleteItemFromOnProgressQueue(ctx, job)
			if err != nil {
				log.Error(err)
			}
			locker.Release(ctx)
		}
	}
}

func (r *RedisWorker) addItemToOnProgressQueue(ctx context.Context, job *ScheduledJob) error {
	log.Debugf("addItemToOnProgressQueue %s args %s", job.JobName, job.Args)

	key := fmt.Sprintf("%s:%s:%s", r.namespace, job.JobName, onProgressSuffixKey)
	// ignore the error, bcs this must be a valid struct
	val, _ := job.marshal()

	return backoff.Retry(r.client.RPush(ctx, key, val).Err, backoff.NewConstantBackOff(time.Millisecond))
}

func (r *RedisWorker) requeueFailedJob(ctx context.Context, job *ScheduledJob) error {
	log.Debugf("requeue failed job %s args %s", job.JobName, job.Args)
	// ignore the error, bcs this must be a valid struct
	val, _ := job.marshal()

	key := fmt.Sprintf("%s:%s", r.namespace, job.JobName)
	onProgressKey := fmt.Sprintf("%s:%s:%s", r.namespace, job.JobName, onProgressSuffixKey)

	ops := func() error {
		_, err := r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.RPush(ctx, key, val)
			pipe.LPop(ctx, onProgressKey)
			return nil
		})
		return err
	}

	return backoff.Retry(ops, backoff.NewConstantBackOff(time.Millisecond))
}

func (r *RedisWorker) deleteItemFromOnProgressQueue(ctx context.Context, job *ScheduledJob) error {
	log.Debugf("deleteItemFromOnProgressQueue %s args %s", job.JobName, job.Args)

	onProgressKey := fmt.Sprintf("%s:%s:%s", r.namespace, job.JobName, onProgressSuffixKey)

	return backoff.Retry(r.client.LPop(ctx, onProgressKey).Err, backoff.NewConstantBackOff(time.Millisecond))
}
