package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(id int, duration time.Duration, wg *sync.WaitGroup) {

	fmt.Printf("Worker %d: bắt đầu\n", id)

	time.Sleep(duration)

	fmt.Printf("Worker %d: hoàn thành sau %v\n", id, duration)
	defer wg.Done()

}

func main() {
	start := time.Now()

	var wg sync.WaitGroup

	wg.Add(3)

	go worker(1, 3*time.Second, &wg)
	go worker(2, 1*time.Second, &wg)
	go worker(3, 2*time.Second, &wg)

	wg.Wait()

	fmt.Printf("Tổng thời gian: %v\n", time.Since(start))
}
