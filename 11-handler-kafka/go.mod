// Tên module Go cho chương trình demo trong thư mục này.
module handler-kafka

// Phiên bản Go mà module sử dụng.
go 1.26.6

// Client Go dùng để ghi và đọc message từ Kafka.
require github.com/segmentio/kafka-go v0.4.51

// Các thư viện nén được kafka-go kéo vào như dependency gián tiếp.
require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
