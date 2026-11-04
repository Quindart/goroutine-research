// Chương trình demo Kafka chạy ở chế độ producer hoặc consumer.
// Đây là package main để `go run .` có thể chạy trực tiếp chương trình.
package main

import (
	// context truyền thời hạn hủy xuống các lời gọi Kafka đang chờ.
	"context"
	// encoding/json đổi struct Go thành JSON để Kafka lưu trong value.
	"encoding/json"
	// errors tạo lỗi đầu vào và kiểm tra lỗi khi consumer bị hủy.
	"errors"
	// flag đọc các tùy chọn như -count, -topic và -strategy.
	"flag"
	// fmt tạo chuỗi, in tiến độ và bọc thông tin lỗi.
	"fmt"
	// os đọc tham số dòng lệnh, biến môi trường và ghi lỗi ra stderr.
	"os"
	// sync.WaitGroup chờ các goroutine consumer hoàn tất.
	"sync"
	// sync/atomic đếm tổng message an toàn giữa nhiều goroutine.
	"sync/atomic"
	// time đặt timeout, chờ giữa các lần thử và đo thời gian chạy.
	"time"

	// kafka-go cung cấp Kafka writer, reader, balancer và API tạo topic.
	"github.com/segmentio/kafka-go"
)

const (
	// Go chạy trên máy host sẽ kết nối đến cổng host được Compose mở.
	defaultBroker = "localhost:19092"
	// Topic mặc định để producer ghi và consumer đọc message.
	defaultTopic = "demo-million-messages"
	// Số message mặc định của demo.
	defaultCount = 1_000_000
	// Topic mặc định có sáu partition để quan sát cách phân phối.
	defaultPartitions = 6
	// Mỗi lần tạo slice và gọi WriteMessages xử lý tối đa 5.000 message.
	writeBatchSize = 5_000
)

// payload là cấu trúc dữ liệu sẽ được encode thành JSON trong message value.
type payload struct {
	// ID là số thứ tự message, bắt đầu từ 0.
	ID int `json:"id"`
	// UserID tạo 100 khóa lặp lại: user-000 đến user-099.
	UserID string `json:"user_id"`
	// Text là nội dung dễ nhận biết khi xem message trong UI hoặc terminal.
	Text string `json:"text"`
}

// sample giữ một số trường cần in ra để minh họa message đã đọc từ Kafka.
type sample struct {
	// Partition là partition mà Kafka trả message về.
	Partition int
	// Offset là vị trí của message bên trong partition đó.
	Offset int64
	// Key là khóa Kafka, ở demo này chính là user_id.
	Key string
	// Value là JSON gốc dưới dạng chuỗi.
	Value string
}

func main() {
	// Phần tử đầu tiên của os.Args là tên chương trình; cần thêm lệnh produce/consume.
	if len(os.Args) < 2 {
		usage()
		return
	}

	// Lưu lỗi trả về từ lệnh để in thống nhất ở cuối main.
	var err error
	// Chọn chức năng dựa trên đối số đầu tiên sau tên chương trình.
	switch os.Args[1] {
	case "produce":
		// Chuyển các cờ còn lại, ví dụ -count, vào hàm producer.
		err = produce(os.Args[2:])
	case "consume":
		// Chuyển các cờ còn lại vào hàm consumer.
		err = consume(os.Args[2:])
	case "help", "-h", "--help":
		// In hướng dẫn rồi kết thúc thành công.
		usage()
		return
	default:
		// Lệnh không hợp lệ: in cách dùng và thoát với mã lỗi 2.
		usage()
		os.Exit(2)
	}

	// Lỗi trong produce/consume được in ra stderr và báo mã thoát 1.
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// usage in các cú pháp lệnh và cờ có thể dùng.
func usage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . produce [-count 1000000] [-partitions 6] [-strategy round-robin|hash]")
	fmt.Println("  go run . consume [-count 1000000] [-partitions 6] [-sample 2]")
	fmt.Println("  Optional flags: -broker localhost:19092 -topic demo-million-messages")
}

func produce(args []string) error {
	// Mỗi subcommand có FlagSet riêng để cờ produce không ảnh hưởng consume.
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	// count là số message muốn gửi; mặc định là một triệu.
	count := flags.Int("count", defaultCount, "number of messages")
	// partitions là số partition cần tạo nếu topic chưa tồn tại.
	partitions := flags.Int("partitions", defaultPartitions, "topic partition count")
	// broker lấy từ cờ -broker, mặc định đọc KAFKA_BROKERS hoặc localhost:19092.
	broker := flags.String("broker", brokerAddress(), "Kafka bootstrap address")
	// topic chọn nơi ghi message.
	topic := flags.String("topic", defaultTopic, "Kafka topic")
	// round-robin chia đều; hash chọn partition dựa trên key của message.
	strategy := flags.String("strategy", "round-robin", "partition strategy: round-robin or hash")
	// Parse gán các giá trị truyền trên command line vào những biến cờ ở trên.
	if err := flags.Parse(args); err != nil {
		return err
	}
	// Không cho phép chạy với số message hoặc số partition bằng 0/âm.
	if *count < 1 || *partitions < 1 {
		return errors.New("count and partitions must be positive")
	}

	// Writer gọi balancer cho từng message để quyết định partition đích.
	var balancer kafka.Balancer
	switch *strategy {
	case "round-robin":
		// Luân phiên qua các partition; key không quyết định partition ở chế độ này.
		balancer = &kafka.RoundRobin{}
	case "hash":
		// Hash key; cùng một user_id sẽ được đưa vào cùng partition.
		balancer = &kafka.Hash{}
	default:
		// Báo lỗi nếu người dùng nhập một chiến lược không được hỗ trợ.
		return fmt.Errorf("unknown strategy %q: use round-robin or hash", *strategy)
	}

	// Timeout chung tối đa 20 phút cho bước tạo topic và gửi message.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	// Giải phóng timer/tài nguyên context khi hàm kết thúc.
	defer cancel()
	// Đảm bảo topic tồn tại trước khi writer bắt đầu gửi.
	if err := ensureTopic(ctx, *broker, *topic, *partitions); err != nil {
		return err
	}

	// Writer quản lý kết nối, chia message theo partition và gom batch gửi Kafka.
	writer := &kafka.Writer{
		// Địa chỉ Kafka mà tiến trình Go trên host kết nối tới.
		Addr: kafka.TCP(*broker),
		// Writer này chỉ ghi vào một topic.
		Topic: *topic,
		// Chiến lược chọn partition đã được tạo phía trên.
		Balancer: balancer,
		// Tối đa 1.000 message trong một batch của mỗi partition.
		BatchSize: 1_000,
		// Không đợi quá 100ms để gửi batch chưa đầy.
		BatchTimeout: 100 * time.Millisecond,
		// Chờ mọi replica trong ISR xác nhận; demo có replication factor 1.
		RequiredAcks: kafka.RequireAll,
	}

	// Bắt đầu đo thời gian gửi, không tính khởi động Kafka hay tạo topic.
	started := time.Now()
	// Chia tổng số message thành các lát tối đa 5.000 message để dùng ít RAM.
	for first := 0; first < *count; first += writeBatchSize {
		// last là chỉ số kết thúc không bao gồm trong batch hiện tại.
		last := first + writeBatchSize
		// Batch cuối có thể ngắn hơn 5.000, không vượt quá count.
		if last > *count {
			last = *count
		}

		// Tạo slice chứa các Kafka message của batch hiện tại.
		messages := make([]kafka.Message, 0, last-first)
		// Tạo từng message với id từ first đến last-1.
		for id := first; id < last; id++ {
			// Dùng 100 user_id lặp lại để quan sát ảnh hưởng của key khi chọn hash.
			userID := fmt.Sprintf("user-%03d", id%100)
			// Đổi struct thành JSON bytes để làm message value.
			value, err := json.Marshal(payload{
				ID:     id,
				UserID: userID,
				Text:   fmt.Sprintf("message-%07d", id),
			})
			if err != nil {
				// Đóng writer trước khi trả lỗi encode.
				_ = writer.Close()
				// %w giữ lỗi gốc để caller có thể kiểm tra tiếp.
				return fmt.Errorf("encode message %d: %w", id, err)
			}
			// Kafka message có key dùng cho hash và value là JSON payload.
			messages = append(messages, kafka.Message{
				Key:   []byte(userID),
				Value: value,
			})
		}

		// Gửi đồng bộ batch này; lỗi chỉ ra nếu message không ghi được.
		if err := writer.WriteMessages(ctx, messages...); err != nil {
			// Giải phóng tài nguyên writer khi gửi thất bại.
			_ = writer.Close()
			// Ghi rõ khoảng id của batch bị lỗi.
			return fmt.Errorf("write messages %d-%d: %w", first, last-1, err)
		}
		// In tiến độ mỗi 100.000 message và luôn in batch cuối.
		if last%100_000 == 0 || last == *count {
			fmt.Printf("produced %d/%d\n", last, *count)
		}
	}
	// Close flush các batch còn tồn và đóng kết nối Kafka.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}

	// Tính thời gian và throughput trung bình của producer.
	duration := time.Since(started)
	fmt.Printf("done: topic=%s count=%d strategy=%s duration=%s rate=%.0f msg/s\n",
		*topic, *count, *strategy, duration.Round(time.Millisecond), float64(*count)/duration.Seconds())
	return nil
}

func consume(args []string) error {
	// Khai báo các cờ riêng cho subcommand consume.
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	// count là tổng message cần đọc rồi dừng.
	count := flags.Int("count", defaultCount, "number of messages to read")
	// partitions phải khớp số partition của topic đã được producer tạo.
	partitions := flags.Int("partitions", defaultPartitions, "topic partition count")
	// sample giới hạn số message mẫu in ra cho mỗi partition.
	sampleLimit := flags.Int("sample", 2, "sample messages to print per partition")
	// broker và topic phải khớp với producer.
	broker := flags.String("broker", brokerAddress(), "Kafka bootstrap address")
	topic := flags.String("topic", defaultTopic, "Kafka topic")
	// Đọc và kiểm tra cờ trước khi tạo reader.
	if err := flags.Parse(args); err != nil {
		return err
	}
	// count/partitions phải dương; sampleLimit có thể bằng 0 nhưng không âm.
	if *count < 1 || *partitions < 1 || *sampleLimit < 0 {
		return errors.New("count and partitions must be positive; sample cannot be negative")
	}

	// Timeout tổng cho lượt đọc; cancel dùng để dừng tất cả reader cùng lúc.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	// total được cập nhật đồng thời bởi nhiều goroutine nên dùng atomic.
	var total atomic.Int64
	// Mỗi goroutine sở hữu một phần tử counts theo partition của nó.
	counts := make([]int64, *partitions)
	// Lưu tối đa sampleLimit message mẫu cho từng partition.
	samples := make([][]sample, *partitions)
	// Mỗi worker có thể gửi một lỗi; buffer tránh worker bị kẹt khi gửi lỗi.
	errs := make(chan error, *partitions)
	// WaitGroup giúp main chờ các goroutine consumer hoàn tất.
	var workers sync.WaitGroup
	// Bắt đầu đo thời gian đọc.
	started := time.Now()

	// Tạo một reader riêng cho từng partition để thống kê rõ từng partition.
	for partition := 0; partition < *partitions; partition++ {
		// Báo WaitGroup có thêm một worker.
		workers.Add(1)
		// Truyền partition làm tham số để mỗi goroutine giữ đúng số partition.
		go func(partition int) {
			// Báo worker hoàn tất dù thoát do đủ message hay lỗi.
			defer workers.Done()
			// Reader được gán trực tiếp vào một partition, không dùng consumer group.
			reader := kafka.NewReader(kafka.ReaderConfig{
				// Broker dùng để tìm metadata và kết nối tới Kafka.
				Brokers: []string{*broker},
				// Topic cần đọc.
				Topic: *topic,
				// Partition cụ thể mà goroutine này phụ trách.
				Partition: partition,
				// Đọc từ offset sớm nhất còn được Kafka lưu.
				StartOffset: kafka.FirstOffset,
				// Có thể trả dữ liệu ngay khi có ít nhất 1 byte.
				MinBytes: 1,
				// Giới hạn tối đa 10 MB dữ liệu mỗi lần fetch.
				MaxBytes: 10e6,
			})
			// Giải phóng reader khi goroutine kết thúc.
			defer reader.Close()

			// Đọc lặp đến khi đủ tổng message, context bị hủy hoặc gặp lỗi.
			for {
				// ReadMessage chờ message hoặc trả lỗi khi context kết thúc.
				message, err := reader.ReadMessage(ctx)
				if err != nil {
					// Cancel/deadline là cách dừng bình thường sau khi đủ message.
					// Lỗi khác được chuyển về goroutine main để báo cho người dùng.
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errs <- fmt.Errorf("read partition %d: %w", partition, err)
					}
					return
				}

				// Tăng bộ đếm riêng của partition này.
				counts[partition]++
				// Chỉ giữ các mẫu đầu tiên để không lưu cả triệu message trong RAM.
				if len(samples[partition]) < *sampleLimit {
					samples[partition] = append(samples[partition], sample{
						Partition: message.Partition,
						Offset:    message.Offset,
						Key:       string(message.Key),
						Value:     string(message.Value),
					})
				}
				// Tăng tổng đếm an toàn giữa các goroutine; đủ count thì dừng tất cả.
				if total.Add(1) >= int64(*count) {
					cancel()
					return
				}
			}
			// Truyền giá trị partition của vòng lặp vào goroutine.
		}(partition)
	}

	// Chờ mọi reader kết thúc trước khi main đọc counts và samples.
	workers.Wait()
	// Không worker nào gửi thêm lỗi sau Wait, nên main có thể đóng channel.
	close(errs)
	// Nếu một reader gặp lỗi Kafka thật, trả lỗi đó về main.
	for err := range errs {
		if err != nil {
			return err
		}
	}
	// Xác nhận đúng số message yêu cầu đã được đọc.
	if got := total.Load(); got != int64(*count) {
		return fmt.Errorf("read %d messages, expected %d; check topic and flags", got, *count)
	}

	// In tổng kết và thời gian đọc.
	fmt.Printf("done: topic=%s count=%d duration=%s\n", *topic, total.Load(), time.Since(started).Round(time.Millisecond))
	// In số message và các mẫu đã giữ cho từng partition.
	for partition := range counts {
		fmt.Printf("partition=%d count=%d\n", partition, counts[partition])
		for _, message := range samples[partition] {
			fmt.Printf("  offset=%d key=%s value=%s\n", message.Offset, message.Key, message.Value)
		}
	}
	return nil
}

func ensureTopic(ctx context.Context, broker, topic string, partitions int) error {
	// Giữ lỗi gần nhất để trả về nếu mọi lần kết nối/tạo topic đều thất bại.
	var lastErr error
	// Kafka container có thể chưa sẵn sàng ngay sau docker compose up.
	for attempt := 1; attempt <= 30; attempt++ {
		// Kết nối đến broker bằng TCP, dùng chung context có timeout.
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			// Tạo topic với số partition yêu cầu và replication factor 1.
			// Nếu topic đã tồn tại, Kafka không tạo lại và không đổi cấu hình cũ.
			err = conn.CreateTopics(kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			})
			// Không còn cần kết nối admin sau khi gửi yêu cầu tạo topic.
			_ = conn.Close()
			if err == nil {
				// Topic đã sẵn sàng hoặc đã tồn tại.
				return nil
			}
		}
		// Lưu lỗi để báo sau lần thử cuối.
		lastErr = err
		// Dừng sớm nếu context timeout/cancel; nếu chưa thì chờ một giây rồi thử lại.
		select {
		case <-ctx.Done():
			return fmt.Errorf("connect to Kafka at %s: %w", broker, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	// Hết 30 lần thử mà vẫn lỗi: trả topic, địa chỉ và lỗi gần nhất.
	return fmt.Errorf("create topic %q after waiting for Kafka: %w", topic, lastErr)
}

// brokerAddress ưu tiên biến môi trường, nếu không có thì dùng địa chỉ mặc định.
func brokerAddress() string {
	// KAFKA_BROKERS cho phép đổi broker mà không cần sửa source.
	if broker := os.Getenv("KAFKA_BROKERS"); broker != "" {
		return broker
	}
	// localhost:19092 là cổng host được khai báo trong compose.yaml.
	return defaultBroker
}
