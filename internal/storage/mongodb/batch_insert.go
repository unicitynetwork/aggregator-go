package mongodb

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type finalizationInsertOptions struct {
	chunkSize int
	workers   int
}

func insertManyFinalizationBatch(
	ctx context.Context,
	collection *mongo.Collection,
	docs []interface{},
	opts finalizationInsertOptions,
) error {
	if len(docs) == 0 {
		return nil
	}
	if opts.chunkSize <= 0 || opts.chunkSize >= len(docs) {
		return ignoreDuplicateInsertError(collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)))
	}
	if opts.workers <= 1 {
		for start := 0; start < len(docs); start += opts.chunkSize {
			if err := ctx.Err(); err != nil {
				return err
			}
			end := min(start+opts.chunkSize, len(docs))
			if err := ignoreDuplicateInsertError(collection.InsertMany(ctx, docs[start:end], options.InsertMany().SetOrdered(false))); err != nil {
				return err
			}
		}
		return nil
	}

	type chunk struct {
		start int
		end   int
	}

	chunkCount := (len(docs) + opts.chunkSize - 1) / opts.chunkSize
	workers := min(opts.workers, chunkCount)
	jobs := make(chan chunk)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	setFirstErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	getFirstErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}

	for range workers {
		wg.Go(func() {
			for job := range jobs {
				if err := ctx.Err(); err != nil {
					setFirstErr(err)
					continue
				}
				err := ignoreDuplicateInsertError(collection.InsertMany(ctx, docs[job.start:job.end], options.InsertMany().SetOrdered(false)))
				setFirstErr(err)
			}
		})
	}

queue:
	for start := 0; start < len(docs); start += opts.chunkSize {
		if err := ctx.Err(); err != nil {
			setFirstErr(err)
			break
		}
		if getFirstErr() != nil {
			break
		}
		select {
		case jobs <- chunk{start: start, end: min(start+opts.chunkSize, len(docs))}:
		case <-ctx.Done():
			setFirstErr(ctx.Err())
			break queue
		}
	}
	close(jobs)
	wg.Wait()

	return getFirstErr()
}

func ignoreDuplicateInsertError(_ *mongo.InsertManyResult, err error) error {
	if err == nil || mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}
