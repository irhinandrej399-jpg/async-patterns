package main

func main() {}
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
