package main

import "sync"

func main() {}

// синхронная отправка
func tee(ch <-chan int) (chan int, chan int) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	go func() {
		for v := range ch {
			ch1 <- v
			ch2 <- v
		}
		close(ch1)
		close(ch2)
	}()
	return ch1, ch2
}
func asyncTee(ch <-chan int) (chan int, chan int) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	wg := &sync.WaitGroup{}
	go func() {
		for v := range ch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch1 <- v
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch2 <- v
			}()
		}
		wg.Wait()
		close(ch1)
		close(ch2)
	}()
	return ch1, ch2
}
