package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-redis/redis/v8"
)

type (
	Consumer struct {
		redisClient *redisClient

		lastMessageID string
		eofReached    bool

		readbuf  []byte
		consumed int
	}
)

func NewConsumer(streamUrl string) (io.ReadCloser, error) {
	r, err := newRedisClient(streamUrl)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		redisClient:   r,
		lastMessageID: "0",
	}, nil
}

func (r *Consumer) Read(p []byte) (n int, err error) {
	n = copy(p, r.readbuf[r.consumed:])
	r.consumed += n

	if err = r.eof(); n != 0 || err != nil {
		return n, err
	}

	r.consumed = 0
	r.readbuf = r.readbuf[:0]

	ctx := context.Background()

	cmd := r.redisClient.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{r.redisClient.stream, r.lastMessageID},
		Block:   5 * time.Second,
	})

	streams, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redis: xread: %w", err)
	}

streams:
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			ev, ok := msg.Values["event_type"].(string)
			if !ok {
				return 0, fmt.Errorf("no event_type in message: %v", msg)
			}

			if ev == "disconnect" {
				r.eofReached = true

				break streams
			}

			if ev != "log" {
				return 0, fmt.Errorf("unexpected event_type: %v", ev)
			}

			text, ok := msg.Values["bytes"].(string)
			if !ok {
				return 0, fmt.Errorf("malformed message: no bytes: %v", msg)
			}

			m := copy(p[n:], text)
			n += m

			if m < len(text) {
				r.readbuf = append(r.readbuf, text[m:]...)
			}

			r.lastMessageID = msg.ID
		}
	}

	return n, r.eof()
}

func (r *Consumer) Close() error {
	return r.redisClient.close()
}

func (r *Consumer) eof() error {
	if r.eofReached && r.consumed == len(r.readbuf) {
		return io.EOF
	}

	return nil
}
