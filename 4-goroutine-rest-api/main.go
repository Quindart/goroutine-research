package main

import (
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	withMocks := flag.Bool("mock", false, "start local mock user, order, and notification services")
	flag.Parse()

	if *withMocks {
		startMockService("users", ":8081", "/api/v1/users/me", gin.H{
			"id": 1, "name": "Nguyen Van An", "email": "an@example.com",
		})
		startMockService("orders", ":8082", "/api/v1/orders", gin.H{
			"orders": []gin.H{
				{"id": "ORD-1001", "status": "processing", "total": 1250000},
				{"id": "ORD-1002", "status": "delivered", "total": 890000},
			},
		})
		startMockService("notifications", ":8083", "/api/v1/notifications", gin.H{
			"notifications": []gin.H{
				{"id": "NTF-1", "message": "Đơn hàng ORD-1001 đang được xử lý", "read": false},
				{"id": "NTF-2", "message": "Chào mừng bạn quay trở lại", "read": true},
			},
		})
	}

	r := gin.Default()

	r.GET("/api/v1/dashboard", func(c *gin.Context) {
		ctx := c.Request.Context()

		var (
			user          map[string]any
			orders        map[string]any
			notifications map[string]any
			userErr, orderErr, notificationErr error
		)

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()

			user, userErr = getJSON(
				ctx,
				"http://localhost:8081/api/v1/users/me",
			)
		}()

		go func() {
			defer wg.Done()

			orders, orderErr = getJSON(
				ctx,
				"http://localhost:8082/api/v1/orders",
			)
		}()

		go func() {
			defer wg.Done()

			notifications, notificationErr = getJSON(
				ctx,
				"http://localhost:8083/api/v1/notifications",
			)
		}()

		wg.Wait()

		if userErr != nil ||
			orderErr != nil ||
			notificationErr != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": "Failed to fetch dashboard data",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"user":          user,
			"orders":        orders,
			"notifications": notifications,
		})
	})

	if err := r.Run(":8087"); err != nil {
		log.Fatal(err)
	}
}

func startMockService(name, address, path string, response gin.H) {
	go func() {
		r := gin.New()
		r.Use(gin.Recovery())
		r.GET(path, func(c *gin.Context) {
			c.JSON(http.StatusOK, response)
		})
		server := &http.Server{
			Addr:              address,
			Handler:           r,
			ReadHeaderTimeout: 3 * time.Second,
		}
		log.Printf("mock %s service listening on %s", name, address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("mock %s service: %v", name, err)
		}
	}()
}
