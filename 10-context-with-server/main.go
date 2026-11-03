package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type contextKeyType string

const requestIDKey contextKeyType = "123"

func queryDatabase(ctx context.Context, userID string) (string, error) {
	reqId, _ := ctx.Value(requestIDKey).(string)

	resultCh := make(chan string, 1)
	go func() {
		time.Sleep(time.Second * 2)
		resultCh <- fmt.Sprintf("User_Name_For_%s", userID)

	}()
	select {
	case <-ctx.Done():
		fmt.Printf("[Log ID: %s] DB query cancelled: %v\n", reqId, ctx.Err())
		return "", ctx.Err()
	case res := <-resultCh:
		return res, nil
	}
}

func handlerRequest(w http.ResponseWriter, r *http.Request) {
	parentCtx := r.Context()
	reqID := "REQ-9999-XYZ"

	ctx := context.WithValue(parentCtx, requestIDKey, reqID)
	// QUAN TRỌNG: Thiết lập Timeout tối đa 1 giây cho việc gọi DB/API bên ngoài
	// Dù client có kiên nhẫn chờ, server cũng chỉ cho phép tối đa 1s để lấy dữ liệu.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	fmt.Printf("[Log ID: %s] Bắt đầu xử lý request lấy user...\n", reqID)

	userName, err := queryDatabase(ctx, "123")
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// Lỗi do quá 1 giây mà DB chưa trả kết quả
			http.Error(w, "Gateway Timeout: Server phản hồi quá chậm", http.StatusGatewayTimeout)
			fmt.Printf("[Log ID: %s] Lỗi: Timeout khi gọi DB\n", reqID)
			return
		}
		// Lỗi do client hủy kết nối giữa chừng
		http.Error(w, "Request cancelled", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Hello, %s!", userName)
}

func main() {
	http.HandleFunc("/getUser", handlerRequest)
	fmt.Println("API Server đang chạy tại cổng :8080...")
	fmt.Println("API Server đang chạy tại cổng :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}

}
