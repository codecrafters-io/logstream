package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

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

	parts := strings.Split(*streamUrl, "/")
	streamKey := parts[len(parts)-1]
	redisUrl := strings.Join(parts[0:len(parts)-1], "/")

	args := flag.Args()

	if args[0] == "follow" {
		consumer, err := NewConsumer(redisUrl, streamKey)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		bytes, err := io.Copy(os.Stdout, consumer)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Read %d bytes\n", bytes)
	} else if args[0] == "run" {
		fmt.Printf("Streaming logs from/to %s\n", *streamUrl)
	} else {
		fmt.Printf("Invalid args! %v\n", args)
	}
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
	cmd := c.redisClient.XRead(&redis.XReadArgs{
		Streams: []string{c.streamKey, c.lastMessageID},
		Block:   1 * time.Second,
	})

	streams, err := cmd.Result()
	if err == redis.Nil {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	for _, stream := range streams {
		for _, message := range stream.Messages {
			if message.Values["event_type"].(string) == "disconnect" {
				return 0, io.EOF
			}
			readableBytes := []byte(message.Values["bytes"].(string))
			readableBytes = readableBytes[c.bytesReadOfCurrentMessage:]

			// When we have lesser than what's asked, perform complete read
			if len(readableBytes) <= len(p) {
				for i, byte := range readableBytes {
					p[i] = byte
				}

				c.lastMessageID = message.ID
				c.bytesReadOfCurrentMessage = 0

				return len(readableBytes), nil

				// readableBytes is greater than len(p). Let's read whatever is possible
			} else {
				for i, _ := range p {
					p[i] = readableBytes[i]
				}

				c.bytesReadOfCurrentMessage += len(p)

				return len(p), nil
			}
		}
	}

	panic("Shouldn't hit this!")
}
