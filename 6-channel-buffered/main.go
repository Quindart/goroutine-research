package main

import "fmt"

func main() {
	ch := make(chan int, 3)
	// defer close(ch)
	ch <- 10
	ch <- 3
	ch <- 20
	fmt.Println("here--->")
	close(ch)
	for value := range ch {
		fmt.Println(value)
	}
	// for i := 1; i <= 2; i++ {
	// 	fmt.Println(<-ch)
	// }
}
