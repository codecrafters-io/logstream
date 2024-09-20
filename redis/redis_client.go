package redis

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-redis/redis/v8"
)

type redisClient struct {
	client *redis.Client
	stream string
}

func newRedisClient(urlStream string) (*redisClient, error) {
	u, err := url.Parse(urlStream)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	fs := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })

	if len(fs) == 1 {
		fs = []string{"0", fs[0]} // Default db is 0
	}

	if len(fs) != 2 {
		return nil, fmt.Errorf("url path must be /db/stream_id")
	}

	u.Path = "/" + fs[0]

	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("parse url query params: %w", err)
	}

	if !q.Has("dial_timeout") {
		q.Set("dial_timeout", "30s")
	}

	u.RawQuery = q.Encode()

	opts, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	r := &redisClient{
		client: redis.NewClient(opts),
		stream: fs[1],
	}

	return r, nil
}

func (r *redisClient) xadd(values ...string) error {
	ctx := context.Background()

	cmd := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream,
		ID:     "*", // Maybe we can do better than this?
		Values: values,
	})

	_, err := cmd.Result()
	if err != nil {
		return fmt.Errorf("redis: xadd: %w", err)
	}

	return nil
}

func (r *redisClient) close() error {
	return r.client.Close()
}
