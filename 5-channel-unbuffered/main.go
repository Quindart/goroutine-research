package main

import (
	"fmt"
)

func main() {
	ch := make(chan int)
	go func() {
		defer close(ch)
		ch <- 12
		ch <- 23
		ch <- 30
		fmt.Println("---> end")
	}()
	// value := <-ch
	// time.Sleep(1 * time.Second)
	fmt.Println(<-ch)
	fmt.Println(<-ch)
}
