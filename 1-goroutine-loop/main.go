package main

import (
	"fmt"
	"time"
)

func main() {

	for i := 1; i <= 100; i++ {
		go func(value int) {
			fmt.Println(value)
		}(i)
	}
	time.Sleep(time.Second * 10)
}
