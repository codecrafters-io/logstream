package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"time"

	"github.com/go-redis/redis/v7"
)

func main() {
	streamUrl := flag.String("url", "", "A logstream URL. Example: redis://localhost:6379/0/<stream_id>")
	flag.Parse()

	if *streamUrl == "" {
		fmt.Println("Expected --url to be set!")
		os.Exit(1)
	}

	fmt.Printf("Streaming logs from/to %s\n", *streamUrl)
}

type Producer struct {
	redisClient *redis.Client
	streamKey   string
}

func NewProducer(redisUrl string, streamKey string) (*Producer, error) {
	opts, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, err
	}

	redisClient := redis.NewClient(opts)

	// Delete the key first
	cmd := redisClient.Del(streamKey)
	_, err = cmd.Result()
	if err != nil {
		return nil, err
	}

	return &Producer{
		redisClient: redisClient,
		streamKey:   streamKey,
	}, nil
}

type Consumer struct {
	redisClient               *redis.Client
	streamKey                 string
	lastMessageID             string
	bytesReadOfCurrentMessage int
}

func NewConsumer(redisUrl string, streamKey string) (*Consumer, error) {
	opts, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		redisClient:   redis.NewClient(opts),
		streamKey:     streamKey,
		lastMessageID: "0",
	}, nil
}

func (c *Producer) Write(p []byte) (int, error) {
	cmd := c.redisClient.XAdd(&redis.XAddArgs{
		Stream: c.streamKey,
		ID:     "*", // Maybe we can do better than this?
		Values: map[string]interface{}{
			"event_type": "log",
			"bytes":      string(p),
		},
	})
	_, err := cmd.Result()
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (c *Producer) Close() error {
	cmd := c.redisClient.XAdd(&redis.XAddArgs{
		Stream: c.streamKey,
		ID:     "*", // Maybe we can do better than this?
		Values: map[string]interface{}{
			"event_type": "disconnect",
		},
	})
	_, err := cmd.Result()
	return err
}

func (c *Consumer) Read(p []byte) (int, error) {
	fmt.Println("Read called")
	cmd := c.redisClient.XRead(&redis.XReadArgs{
		Streams: []string{c.streamKey, c.lastMessageID},
		Block:   1 * time.Second,
	})

	streams, err := cmd.Result()
	if err == redis.Nil {
		// Hit timeout
		fmt.Println("Hit timeout", err)
		return 0, nil
	}

	if err != nil {
		fmt.Printf("Read returns error %v\n", err)
		return 0, err
	}

	for _, stream := range streams {
		fmt.Printf("Found %v messages\n", len(stream.Messages))
		for _, message := range stream.Messages {
			fmt.Printf("Message ID: %v\n", message.ID)
			if message.Values["event_type"].(string) == "disconnect" {
				fmt.Println("Hit disconnect", err)
				return 0, io.EOF
			}
			readableBytes := []byte(message.Values["bytes"].(string))
			readableBytes = readableBytes[c.bytesReadOfCurrentMessage:]

			fmt.Printf("Readable: %v, p: %v!\n", len(readableBytes), len(p))

			// When we have lesser than what's asked, perform complete read
			if len(readableBytes) <= len(p) {
				for i, byte := range readableBytes {
					p[i] = byte
				}

				c.lastMessageID = message.ID
				c.bytesReadOfCurrentMessage = 0

				fmt.Printf("Read %v bytes\n", len(readableBytes))
				return len(readableBytes), nil

				// readableBytes is greater than len(p). Let's read whatever is possible
			} else {
				for i, _ := range p {
					p[i] = readableBytes[i]
				}

				c.bytesReadOfCurrentMessage += len(p)

				fmt.Printf("Read %v bytes\n", len(p))
				return len(p), nil
			}
		}
	}

	fmt.Println("Read returns normally")
	return 8, nil
}
