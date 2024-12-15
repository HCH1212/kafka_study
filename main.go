package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	reader *kafka.Reader
	topic  = "user_click"
)

// 生产消息
func writeKafka(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone, // leader和follower都不需要等待返回ack
		AllowAutoTopicCreation: true,              // 第一次可以自动创建topic，实际生产环境应该为false，由运维人员创建
	}
	defer writer.Close()

	for i := 0; i < 3; i++ { // 最多重试三次
		err := writer.WriteMessages(ctx,
			kafka.Message{Key: []byte("1"), Value: []byte("my data number one")}, // 五条消息具有原子性
			kafka.Message{Key: []byte("2"), Value: []byte("my data number two")},
			kafka.Message{Key: []byte("3"), Value: []byte("my data number three")},
			kafka.Message{Key: []byte("1"), Value: []byte("my data number four")},
			kafka.Message{Key: []byte("1"), Value: []byte("my data number five")},
		)
		if err != nil {
			if errors.Is(err, kafka.LeaderNotAvailable) { // 如果一开始topic不存在，那么第一次写入一定会失败（第一次相当于创建topic）
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				fmt.Println("写入消息失败：", err)
			}
		} else {
			break
		}
	}
}

// 消费消息
func readKafka(ctx context.Context) {
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		CommitInterval: time.Second,
		GroupID:        "red_rock",
		StartOffset:    kafka.FirstOffset, // 指定开始消费的位置，仅对新消费方创建时有效
	})

	//defer reader.Close() // 正常情况无限循环，要关闭的话只能直接关闭进程，无论如何都不会执行这里，所以需要72行代码

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("kafka read err:", err)
			break
		} else {
			fmt.Printf("topic: %s, partition: %d, offset: %d, key: %s, valua: %s\n",
				message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		}
	}
}

// 需要监听信息2、15，当收到信号关闭reader
func listenSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c // 没有会堵塞
	fmt.Println("接收到信号：", sig.String())
	if reader != nil {
		_ = reader.Close()
	}
	os.Exit(0)
}

func main() {
	ctx := context.Background()
	// 生产方
	//writeKafka(ctx)

	// 消费方
	go listenSignal()
	readKafka(ctx)
}
