package main

import (
	"fmt"
	"time"
)

func demoLoop(number int) {

	for i := 1; i <= 5; i++ {
		fmt.Println("looop numer", number)
		fmt.Println(i)
	}
}

func main() {
	go demoLoop(1)
	go demoLoop(2)
	go demoLoop(3)

	time.Sleep(time.Second)
}
