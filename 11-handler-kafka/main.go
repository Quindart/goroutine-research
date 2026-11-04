package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBroker     = "localhost:19092"
	defaultTopic      = "demo-million-messages"
	defaultCount      = 1_000_000
	defaultPartitions = 6
	writeBatchSize    = 5_000
)

type payload struct {
	ID     int    `json:"id"`
	UserID string `json:"user_id"`
	Text   string `json:"text"`
}

type sample struct {
	Partition int
	Offset    int64
	Key       string
	Value     string
}

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	var err error
	switch os.Args[1] {
	case "produce":
		err = produce(os.Args[2:])
	case "consume":
		err = consume(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . produce [-count 1000000] [-partitions 6] [-strategy round-robin|hash]")
	fmt.Println("  go run . consume [-count 1000000] [-partitions 6] [-sample 2]")
	fmt.Println("  Optional flags: -broker localhost:19092 -topic demo-million-messages")
}

func produce(args []string) error {
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	count := flags.Int("count", defaultCount, "number of messages")
	partitions := flags.Int("partitions", defaultPartitions, "topic partition count")
	broker := flags.String("broker", brokerAddress(), "Kafka bootstrap address")
	topic := flags.String("topic", defaultTopic, "Kafka topic")
	strategy := flags.String("strategy", "round-robin", "partition strategy: round-robin or hash")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *count < 1 || *partitions < 1 {
		return errors.New("count and partitions must be positive")
	}

	var balancer kafka.Balancer
	switch *strategy {
	case "round-robin":
		balancer = &kafka.RoundRobin{}
	case "hash":
		balancer = &kafka.Hash{}
	default:
		return fmt.Errorf("unknown strategy %q: use round-robin or hash", *strategy)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := ensureTopic(ctx, *broker, *topic, *partitions); err != nil {
		return err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     balancer,
		BatchSize:    1_000,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}

	started := time.Now()
	for first := 0; first < *count; first += writeBatchSize {
		last := first + writeBatchSize
		if last > *count {
			last = *count
		}

		messages := make([]kafka.Message, 0, last-first)
		for id := first; id < last; id++ {
			userID := fmt.Sprintf("user-%03d", id%100)
			value, err := json.Marshal(payload{
				ID:     id,
				UserID: userID,
				Text:   fmt.Sprintf("message-%07d", id),
			})
			if err != nil {
				_ = writer.Close()
				return fmt.Errorf("encode message %d: %w", id, err)
			}
			messages = append(messages, kafka.Message{
				Key:   []byte(userID),
				Value: value,
			})
		}

		if err := writer.WriteMessages(ctx, messages...); err != nil {
			_ = writer.Close()
			return fmt.Errorf("write messages %d-%d: %w", first, last-1, err)
		}
		if last%100_000 == 0 || last == *count {
			fmt.Printf("produced %d/%d\n", last, *count)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}

	duration := time.Since(started)
	fmt.Printf("done: topic=%s count=%d strategy=%s duration=%s rate=%.0f msg/s\n",
		*topic, *count, *strategy, duration.Round(time.Millisecond), float64(*count)/duration.Seconds())
	return nil
}

func consume(args []string) error {
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	count := flags.Int("count", defaultCount, "number of messages to read")
	partitions := flags.Int("partitions", defaultPartitions, "topic partition count")
	sampleLimit := flags.Int("sample", 2, "sample messages to print per partition")
	broker := flags.String("broker", brokerAddress(), "Kafka bootstrap address")
	topic := flags.String("topic", defaultTopic, "Kafka topic")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *count < 1 || *partitions < 1 || *sampleLimit < 0 {
		return errors.New("count and partitions must be positive; sample cannot be negative")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var total atomic.Int64
	counts := make([]int64, *partitions)
	samples := make([][]sample, *partitions)
	errs := make(chan error, *partitions)
	var workers sync.WaitGroup
	started := time.Now()

	for partition := 0; partition < *partitions; partition++ {
		workers.Add(1)
		go func(partition int) {
			defer workers.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:     []string{*broker},
				Topic:       *topic,
				Partition:   partition,
				StartOffset: kafka.FirstOffset,
				MinBytes:    1,
				MaxBytes:    10e6,
			})
			defer reader.Close()

			for {
				message, err := reader.ReadMessage(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errs <- fmt.Errorf("read partition %d: %w", partition, err)
					}
					return
				}

				counts[partition]++
				if len(samples[partition]) < *sampleLimit {
					samples[partition] = append(samples[partition], sample{
						Partition: message.Partition,
						Offset:    message.Offset,
						Key:       string(message.Key),
						Value:     string(message.Value),
					})
				}
				if total.Add(1) >= int64(*count) {
					cancel()
					return
				}
			}
		}(partition)
	}

	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	if got := total.Load(); got != int64(*count) {
		return fmt.Errorf("read %d messages, expected %d; check topic and flags", got, *count)
	}

	fmt.Printf("done: topic=%s count=%d duration=%s\n", *topic, total.Load(), time.Since(started).Round(time.Millisecond))
	for partition := range counts {
		fmt.Printf("partition=%d count=%d\n", partition, counts[partition])
		for _, message := range samples[partition] {
			fmt.Printf("  offset=%d key=%s value=%s\n", message.Offset, message.Key, message.Value)
		}
	}
	return nil
}

func ensureTopic(ctx context.Context, broker, topic string, partitions int) error {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			err = conn.CreateTopics(kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			})
			_ = conn.Close()
			if err == nil {
				return nil
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("connect to Kafka at %s: %w", broker, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("create topic %q after waiting for Kafka: %w", topic, lastErr)
}

func brokerAddress() string {
	if broker := os.Getenv("KAFKA_BROKERS"); broker != "" {
		return broker
	}
	return defaultBroker
}
