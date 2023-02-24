package redis

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-redis/redis/v8"
)

type (
	Redis struct {
		client *redis.Client
		stream string
	}
)

func newRedis(urlStream string) (*Redis, error) {
	u, err := url.Parse(urlStream)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	fs := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })

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

	r := &Redis{
		client: redis.NewClient(opts),
		stream: fs[1],
	}

	return r, nil
}

func (r *Redis) xadd(values ...string) error {
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

func (r *Redis) Close() error { return r.client.Close() }
