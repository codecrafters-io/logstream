package consumer

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"strings"
	"time"
)

var ctx = context.Background()

type Consumer struct {
	redisClient               *redis.Client
	streamKey                 string
	lastMessageID             string
	logDebug                  func(string)
	bytesReadOfCurrentMessage int
}

func NewConsumer(streamUrl string, logDebug func(string)) (*Consumer, error) {
	redisUrl, streamKey := parseUrl(streamUrl)

	opts, err := redis.ParseURL(redisUrl)
	opts.DialTimeout = time.Second * 30
	if err != nil {
		fmt.Printf("Err: %v\n", err)
		return &Consumer{}, err
	}

	return &Consumer{
		redisClient:   redis.NewClient(opts),
		streamKey:     streamKey,
		lastMessageID: "0",
		logDebug:      logDebug,
	}, nil
}

func (c *Consumer) Read(p []byte) (int, error) {
	c.logDebug("Consumer.Read() called")
	cmd := c.redisClient.XRead(ctx, &redis.XReadArgs{
		Streams: []string{c.streamKey, c.lastMessageID},
		Block:   5 * time.Second,
	})

	streams, err := cmd.Result()
	if err == redis.Nil {
		c.logDebug("-> received nil response.")
		return 0, nil
	}

	if err != nil {
		c.logDebug("-> received err.")
		return 0, err
	}

	c.logDebug("-> reading streams")
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

				c.logDebug("  -> read entire stream into buffer")
				return len(readableBytes), nil

				// readableBytes is greater than len(p). Let's read whatever is possible
			} else {
				for i, _ := range p {
					p[i] = readableBytes[i]
				}

				c.bytesReadOfCurrentMessage += len(p)

				c.logDebug("  -> read partial stream into buffer (stream length greater than buffer)")
				return len(p), nil
			}
		}
	}

	panic("Shouldn't hit this!")
}

// Duplication! This is also present in main.go. Centralize this somewhere.
func parseUrl(streamUrl string) (redisUrl string, streamKey string) {
	parts := strings.Split(streamUrl, "/")
	streamKey = parts[len(parts)-1]
	redisUrl = strings.Join(parts[0:len(parts)-1], "/")
	return
}
