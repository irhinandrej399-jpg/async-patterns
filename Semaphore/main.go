package main

import (
	"context"
	"fmt"
	"sync"
)

func main() {
	semaphore(context.Background(), []func(){func() {
		fmt.Print()
	}, func() {
		fmt.Print()
	}, func() {
		fmt.Print()
	}}, 1)
}
func semaphore(ctx context.Context, jobs []func(), size int) error {
	if size <= 0 {
		return fmt.Errorf("invalid size")
	}
	sem := make(chan struct{}, size)
	wg := &sync.WaitGroup{}
	for i := range jobs {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func() {
					defer func() {
						defer wg.Done()
						<-sem
					}()
					if jobs[i] == nil {
						fmt.Printf("jobs %d invalid! \n", i)
						return
					}
					jobs[i]()
					fmt.Printf("jobs %d complete! \n", i)
				}()
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			}
		}
	}
	wg.Wait()
	close(sem)
	return nil
}

// Канал jobs закрывает отправитель. При закрытии канала или отмене
// контекста semaphoreFromChannel дожидается завершения запущенных задач.
func semaphoreFromChannel(ctx context.Context, jobs <-chan func(), size int) error {
	if size <= 0 {
		return fmt.Errorf("invalid size")
	}
	sem := make(chan struct{}, size)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var job func()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next, ok := <-jobs:
			if !ok {
				return nil
			}
			job = next
		}
		if job == nil {
			fmt.Println("invalid job: nil")
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}
		if err := ctx.Err(); err != nil {
			<-sem
			return err
		}
		wg.Add(1)
		go func(job func()) {
			defer wg.Done()
			defer func() { <-sem }()
			job()
		}(job)
	}
}
