package main

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

// Компактный вариант: принятые задачи выполняются даже после отмены контекста
// Запускается вместо ProceesWithCtx, один раз на экземпляр пула
func (w *workerPool) ProceesWithCtxCompact(ctx context.Context, t time.Duration) error {
	run := func(job func() int, worker *worker) {
		w.wg.Add(1)
		go func() {
			if worker == nil {
				worker = <-w.pool
			}
			defer func() {
				if r := recover(); r != nil {
					log.Printf("worker %d: panic: %v\n%s", worker.id, r, debug.Stack())
				}
				w.pool <- worker
				w.wg.Done()
			}()
			n := job()
			fmt.Printf("worker %d complete job and got %d\n", worker.id, n)
		}()
	}

loop:
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			break loop
		case job := <-w.jobs:
			var worker *worker
			select {
			case worker = <-w.pool:
			case <-ctx.Done():
			}
			run(job, worker)
		}
	}

	w.stopOnce.Do(func() { close(w.done) })
	finished := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(finished)
	}()
	timer := time.NewTimer(t)
	defer timer.Stop()
	select {
	case <-finished:
		return nil
	case <-timer.C:
		return fmt.Errorf("worker pool shutdown timed out after %s: %w", t, context.DeadlineExceeded)
	}
}

type worker struct {
	id int
}
type workerPool struct {
	pool     chan *worker
	jobs     chan func() int
	done     chan struct{}
	wg       *sync.WaitGroup
	stopOnce *sync.Once
}

func NewWorkerPool(n int) (*workerPool, error) {
	if n <= 0 {
		return nil, fmt.Errorf("pool size <= 0")
	}
	p := make(chan *worker, n)
	j := make(chan func() int)
	d := make(chan struct{})
	wg := &sync.WaitGroup{}
	s := sync.Once{}
	for i := range n {
		p <- &worker{id: i}
	}
	return &workerPool{p, j, d, wg, &s}, nil
}

// простая реализация
func (w *workerPool) Procees() {
	wg := &sync.WaitGroup{}
	for job := range w.jobs {
		worker := <-w.pool
		wg.Add(1)
		go func() {
			defer func() {
				w.pool <- worker
				wg.Done()
			}()
			job()
			fmt.Printf("worker %d complete job /n", worker.id)
		}()
	}
	wg.Wait()
}
func (w *workerPool) ProceesWithCtx(ctx context.Context, t time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			ch := make(chan struct{}, 1)
			timer := time.NewTimer(t)
			defer timer.Stop()
			go func() {
				ch <- w.Stop()
			}()
			select {
			case <-timer.C:
				return fmt.Errorf("worker pool shutdown timed out after %s: %w", t, context.DeadlineExceeded)
			case <-ch:
				return nil
			}
		case job := <-w.jobs:
			select {
			case worker := <-w.pool:
				w.wg.Add(1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("worker %d: panic: %v\n%s", worker.id, r, debug.Stack())
						}
						w.pool <- worker
						w.wg.Done()
					}()
					n := job()
					fmt.Printf("worker %d complete job and got %d/n", worker.id, n)
				}()
			case <-ctx.Done():
				w.wg.Add(1)
				go func() {
					worker := <-w.pool
					defer func() {
						if r := recover(); r != nil {
							log.Printf("worker %d: panic: %v\n%s", worker.id, r, debug.Stack())
						}
						w.pool <- worker
						w.wg.Done()
					}()
					n := job()
					fmt.Printf("worker %d complete job and got %d/n", worker.id, n)

				}()
				ch := make(chan struct{}, 1)
				timer := time.NewTimer(t)
				defer timer.Stop()
				go func() {
					ch <- w.Stop()
				}()
				select {
				case <-timer.C:
					return fmt.Errorf("worker pool shutdown timed out after %s: %w", t, context.DeadlineExceeded)
				case <-ch:
					return nil
				}
			}
		}
	}
}
func (w *workerPool) Stop() struct{} {
	w.stopOnce.Do(func() {
		close(w.done)
	})
	w.wg.Wait()
	return struct{}{}
}
func (w *workerPool) Submit(ctx context.Context, task func() int) error {
	if task == nil {
		return fmt.Errorf("invalid task")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return fmt.Errorf("worker done")
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.done:
			return fmt.Errorf("worker done")
		case w.jobs <- task:
			return nil
		}
	}
}
func (w *workerPool) TrySubmit(task func() int) error {
	if task == nil {
		return fmt.Errorf("invalid task")
	}
	select {
	case w.jobs <- task:
		return nil
	default:
	}
	select {
	case <-w.done:
		return fmt.Errorf("pool stoped")
	default:
	}
	return fmt.Errorf("task dont upload")
}
func main() {

}
