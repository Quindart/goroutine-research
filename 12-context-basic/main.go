package main

import (
	"context"
	"fmt"
	"time"
)

func task(ctx context.Context) {
	select {
	case <-ctx.Done():
		fmt.Println("Task cancel")
	case <-time.After(time.Second * 2):
		fmt.Println("Done task")
	}
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	go task(ctx)
	time.Sleep(1 * time.Second)
	cancel()
	time.Sleep(1 * time.Second)
}
