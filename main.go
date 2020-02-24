package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
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

	opts, err := redis.ParseURL(redisUrl)
	opts.DialTimeout = time.Second * 30
	if err != nil {
		fmt.Printf("Err: %v\n", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(opts)

	if args[0] == "follow" {
		consumer, err := NewConsumer(redisClient, streamKey)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		_, err = io.Copy(os.Stdout, consumer)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
	} else if args[0] == "run" {
		producer, err := NewProducer(redisClient, streamKey)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		cmd := exec.Command("sh", []string{"-c", strings.Join(args[1:], " ")}...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		ch := make(chan bool)

		go func() {
			_, err := io.Copy(io.MultiWriter(producer, os.Stdout), stdout)
			if err != nil {
				fmt.Printf("Stdout Err: %v\n", err)
				return
			}
			ch <- true
		}()

		go func() {
			_, err := io.Copy(io.MultiWriter(producer, os.Stderr), stderr)
			if err != nil {
				fmt.Printf("Stderr Err: %v\n", err)
				return
			}
			ch <- true
		}()

		err = cmd.Start()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
		<-ch
		<-ch
		err = cmd.Wait()
		producer.Close()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Invalid args! %v\n", args)
	}
}

type Producer struct {
	redisClient *redis.Client
	streamKey   string
}

func NewProducer(redisClient *redis.Client, streamKey string) (*Producer, error) {
	// Delete the key first
	cmd := redisClient.Del(streamKey)
	_, err := cmd.Result()
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

func NewConsumer(redisClient *redis.Client, streamKey string) (*Consumer, error) {
	return &Consumer{
		redisClient:   redisClient,
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
