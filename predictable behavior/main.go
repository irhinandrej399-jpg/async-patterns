package main

import (
	"context"
	"fmt"
	"time"
)

func slow() int {
	time.Sleep(40 * time.Second)
	return 2
}
func predictableBehavior(t time.Duration) int {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	ch := make(chan int, 0)
	go func() {
		v := slow()
		select {
		case <-ctx.Done():
			return
		default:
			ch <- v
		}
	}()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		return 0
	}
}
func main() {
	fmt.Println(predictableBehavior(5 * time.Second))
}
