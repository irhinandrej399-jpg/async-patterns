package main

import (
	"context"
	"sync"
)

func worker(ch <-chan int, res chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for v := range ch {
		res <- v
	}
}

// создать выходящий канал и вернуть его, обработку каналов делать в фоне
func fanIn(chs ...<-chan int) <-chan int {
	res := make(chan int)
	wg := &sync.WaitGroup{}
	wg.Add(len(chs))
	go func() {
		wg.Wait()
		close(res)
	}()
	go func() {
		for _, ch := range chs {
			go worker(ch, res, wg)
		}
	}()
	return res
}
func workerWithCtx(ctx context.Context, ch <-chan int, res chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-ch:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case res <- v:
			}
		}
	}
}
func fanInWithCtx(ctx context.Context, chs ...<-chan int) <-chan int {
	res := make(chan int)
	wg := &sync.WaitGroup{}
	wg.Add(len(chs))
	go func() {
		wg.Wait()
		close(res)
	}()
	go func() {
		for _, ch := range chs {
			go workerWithCtx(ctx, ch, res, wg)
		}
	}()
	return res
}

type Uniq struct {
	m  map[int]struct{}
	mu *sync.Mutex
}

func workerWithCtxUniq(ctx context.Context, ch <-chan int, res chan<- int, wg *sync.WaitGroup, m Uniq) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-ch:
			if !ok {
				return
			}
			m.mu.Lock()
			if _, ok := m.m[v]; ok {
				m.mu.Unlock()
				continue
			} else {
				m.m[v] = struct{}{}
			}
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case res <- v:
			}
		}
	}
}
func fanInUnique(ctx context.Context, chs ...<-chan int) <-chan int {
	res := make(chan int)
	wg := &sync.WaitGroup{}
	wg.Add(len(chs))
	uniq := Uniq{
		m:  make(map[int]struct{}),
		mu: &sync.Mutex{},
	}
	go func() {
		wg.Wait()
		close(res)
	}()
	go func() {
		for _, ch := range chs {
			go workerWithCtxUniq(ctx, ch, res, wg, uniq)
		}
	}()
	return res
}
func main() {

}
