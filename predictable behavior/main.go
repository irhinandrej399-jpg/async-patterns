package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func slow() int {
	time.Sleep(40 * time.Second)
	return 2
}
func predictableBehavior(t time.Duration, ctx context.Context) (int, error) {
	myCtx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	ch := make(chan int, 1)
	go func() {
		defer close(ch)
		ch <- slow()
	}()
	select {
	case <-myCtx.Done():
		return 0, myCtx.Err()
	case v := <-ch:
		return v, nil
	}
}
func predictableBehaviorWithManyGorutines(urls []string, t time.Duration, ctx context.Context) (int, int, error) {
	myCtx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	ch := make(chan *http.Response, len(urls))
	cl := http.Client{Timeout: 5 * time.Second}
	chErr := make(chan error, len(urls))
	wg := &sync.WaitGroup{}
	for i := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(myCtx, "GET", urls[i], nil)
			if err != nil {
				chErr <- err
				return
			}
			resp, err := cl.Do(req)
			if err != nil {
				chErr <- err
				return
			}
			ch <- resp
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
		close(chErr)
	}()
	errCount := 0
	succesCount := 0
	for {
		select {
		case <-myCtx.Done():
			return succesCount, errCount, myCtx.Err()
		default:
			select {
			case <-myCtx.Done():
				return succesCount, errCount, myCtx.Err()
			case r, ok := <-ch:
				if !ok {
					for i := range chErr {
						_ = i
						errCount++
					}
					if errCount == len(urls) {
						return succesCount, errCount, fmt.Errorf("all urls errors")
					}
					return succesCount, errCount, nil
				}
				defer r.Body.Close()
				succesCount++
			case _, ok := <-chErr:
				if !ok {
					for i := range ch {
						defer i.Body.Close()
						succesCount++
					}
					if errCount == len(urls) {
						return succesCount, errCount, fmt.Errorf("all urls errors")
					}
					return succesCount, errCount, nil
				}
				errCount++
			}
		}
	}
}

// компактная версия с одним каналом и намного проще
func predictableBehaviorWithManyGorutinesCompact(urls []string, t time.Duration, ctx context.Context) (int, int, error) {
	ctx, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	results := make(chan error, len(urls))
	client := http.Client{Timeout: 5 * time.Second}
	for _, url := range urls {
		go func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err == nil {
				var resp *http.Response
				resp, err = client.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
			results <- err
		}()
	}

	successCount, errorCount := 0, 0
	for range urls {
		select {
		case <-ctx.Done():
			return successCount, errorCount, ctx.Err()
		case err := <-results:
			if err != nil {
				errorCount++
			} else {
				successCount++
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return successCount, errorCount, err
	}
	if len(urls) > 0 && errorCount == len(urls) {
		return successCount, errorCount, fmt.Errorf("all urls errors")
	}
	return successCount, errorCount, nil
}

func main() {
	fmt.Println(predictableBehavior(3*time.Second, context.Background()))
}
