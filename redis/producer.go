package redis

import (
	"io"
)

func NewProducer(streamUrl string) (io.WriteCloser, error) {
	redisWriter, err := NewRedisWriter(streamUrl)
	if err != nil {
		return nil, err
	}

	return redisWriter, nil
}
