package main

import (
	"context"
	"fmt"
	"time"
)

func cookPho(ctx context.Context, ch chan string) {
	select {
	case <-ctx.Done():
		fmt.Println("Cancel cook pho")
		return
	case <-time.After(time.Second * 1):
		ch <- "Pho is ready"
	}
}

func cookBunBo(ctx context.Context, ch chan string) {
	select {
	case <-ctx.Done():
		fmt.Println("Cancel cook bun bo")
		return
	case <-time.After(time.Second * 3):
		ch <- "Bun Bo is ready"
	}
}
func main() {
	chPho := make(chan string)
	chBunBo := make(chan string)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go cookPho(ctx, chPho)
	go cookBunBo(ctx, chBunBo)

	for i := 1; i <= 5; i++ {
		select {
		case result := <-chPho:
			println(result)
		case result := <-chBunBo:
			println(result)
		case <-ctx.Done():
			fmt.Println("Cancel cook mon")
			return
		}

	}
}
