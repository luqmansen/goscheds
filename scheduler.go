package goscheds

import (
	"context"
	"encoding/json"
	"time"
)

type Job struct {
	JobName string
	id      string
	//StartedAt is time when the job is started
	StartedAt time.Time
	//Timeout is how long a job should be waited if it is hanging on on_progress queue
	Timeout   time.Duration
	ExecuteAt time.Time
	Args      map[string]interface{}
}

func (j *Job) marshal() ([]byte, error) {
	return json.Marshal(j)
}

type HandlerFunc func(job *Job) error

//SchedulerService is interface that need to be satisfied
//for any persistence layer that will be used for scheduler.
type SchedulerService interface {
	StartScheduler(ctx context.Context)
	Push(ctx context.Context, job *Job) error
	PushScheduled(ctx context.Context, job *Job) error
}
