# Go Goroutine Research

Repository gồm các ví dụ nhỏ để thực hành goroutine, channel và đồng bộ trong Go. Mỗi thư mục là một Go module độc lập, vì vậy hãy chạy lệnh từ chính thư mục của bài tương ứng.

## Yêu cầu

- Go 1.26.6 (phiên bản được khai báo trong các file `go.mod`).
- Bài `4-goroutine-rest-api` cần tải các dependency của Gin khi chạy lần đầu.
- Bài `11-handler-kafka` cần Docker để chạy Kafka và giao diện Kafbat UI local.

## Các bài thực hành

| Thư mục | Nội dung |
| --- | --- |
| `1-goroutine-loop` | Khởi chạy nhiều goroutine trong vòng lặp. |
| `2-many-goroutine` | Chạy đồng thời nhiều hàm và quan sát thứ tự thực thi. |
| `3-goroutine-waitgroup-basic` | Dùng `sync.WaitGroup` để đợi các worker hoàn tất. |
| `4-goroutine-rest-api` | Gọi ba dịch vụ HTTP đồng thời, rồi tổng hợp dữ liệu cho dashboard. Có thể chạy kèm mock service. |
| `5-channel-unbuffered` | Ví dụ gửi và nhận dữ liệu qua unbuffered channel. |
| `6-channel-buffered` | Gửi dữ liệu qua buffered channel rồi đọc bằng `range`. |
| `7-waitgroup-channel` | Khung bài thực hành WaitGroup và channel; hiện chưa có phần triển khai. |
| `8-select-channel` | Dùng `select` để nhận dữ liệu từ nhiều channel. |
| `9-context-implement` | Truyền context và xử lý thời hạn hủy tác vụ. |
| `10-context-with-server` | Dùng context theo request trong HTTP server, truyền request ID và hủy truy vấn giả lập khi hết thời hạn. |
| `11-handler-kafka` | Producer/consumer Go gửi một triệu message, xem phân phối partition và duyệt message bằng Kafbat UI. |

## Cách chạy

Vào thư mục của bài muốn chạy rồi dùng `go run .`. Ví dụ:

```sh
cd 3-goroutine-waitgroup-basic
go run .
```

Với bài REST API, khởi chạy API cùng ba mock service:

```sh
cd 4-goroutine-rest-api
go run . -mock
```

Sau đó gửi request đến `http://localhost:8087/api/v1/dashboard`. Các mock service chạy ở cổng `8081`, `8082` và `8083`; có thể xem thêm request mẫu trong [`4-goroutine-rest-api/client.http`](4-goroutine-rest-api/client.http).

Với bài HTTP server dùng context, chạy server:

```sh
cd 10-context-with-server
go run .
```

Mở terminal khác và gọi endpoint:

```sh
curl http://localhost:8080/getUser
```

Endpoint mô phỏng truy vấn mất 2 giây, trả về tên người dùng khi hoàn tất và dùng context deadline để giới hạn thời gian xử lý.

Với bài Kafka, mở terminal tại thư mục và chạy broker cùng giao diện web:

```sh
cd 11-handler-kafka
docker compose up -d
```

Gửi và đọc một triệu message:

```sh
go run . produce
go run . consume
```

Mở `http://localhost:8080` để xem topic, partition và nội dung message. Hướng dẫn phân phối partition và công cụ theo dõi nằm trong [`11-handler-kafka/README.md`](11-handler-kafka/README.md).

## Lưu ý

Các ví dụ tập trung vào một khái niệm và cách chạy goroutine; chúng không phải thư viện dùng trực tiếp trong ứng dụng production. Một số bài có thời gian chờ cố định để dễ quan sát kết quả.
