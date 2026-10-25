package main

import (
	"reflect"
)

func main() {}

// с активным ожидаем
func fanOut(ch chan int, n int) []chan int {
	if n < 0 {
		return nil
	}
	res := make([]chan int, n)
	for i := range res {
		res[i] = make(chan int)
	}
	go func() {
		for v := range ch {
		loop:
			for {
				for i := range res {
					select {
					case res[i] <- v:
						break loop
					default:
					}
				}
			}
		}
		for i := range res {
			close(res[i])
		}
	}()
	return res
}

// С пассивным ожиданием готовности любого выхода.
func fanOutPassive(ch <-chan int, n int) []chan int {
	if n <= 0 {
		return nil
	}
	res := make([]chan int, n)
	cases := make([]reflect.SelectCase, n)
	for i := range res {
		res[i] = make(chan int)
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(res[i]),
		}
	}
	go func() {
		for v := range ch {
			for i := range cases {
				cases[i].Send = reflect.ValueOf(v)
			}
			reflect.Select(cases)
		}
		for _, out := range res {
			close(out)
		}
	}()
	return res
}

// Синхронная отправка по кругу: медленный получатель задерживает распределение.
func fanOutSync(ch <-chan int, n int) []chan int {
	if n <= 0 {
		return nil
	}
	res := make([]chan int, n)
	for i := range res {
		res[i] = make(chan int)
	}
	go func() {
		i := 0
		for v := range ch {
			res[i] <- v
			i = (i + 1) % n
		}
		for _, out := range res {
			close(out)
		}
	}()
	return res
}
