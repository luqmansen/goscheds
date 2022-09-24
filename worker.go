package goscheds

import "context"

type WorkerService interface {
	RegisterHandler(name string, fn HandlerFunc)
	ProcessJob(ctx context.Context)
}
