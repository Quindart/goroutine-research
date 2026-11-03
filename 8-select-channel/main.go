package main

import (
	"fmt"
	"time"
)

// select giúp tránh chờ channel lẫn nhau
func main() {
	c1 := make(chan interface{})
	c2 := make(chan interface{})

	go func() {
		time.Sleep(2 * time.Second)
		c1 <- 1
	}()
	go func() {
		time.Sleep(5 * time.Second)
		c2 <- 2
	}()
	// fmt.Println(<-c1)
	// fmt.Println(<-c2)
	for i := 1; i <= 2; i++ {
		select {
		case data := <-c1:
			fmt.Println("data", data)
		case data := <-c2:
			fmt.Println("data", data)
		}
	}

}
