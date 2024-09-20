package redis

import (
	"github.com/rohitpaulk/asyncwriter"
)

type Producer struct {
	writer *asyncwriter.AsyncWriter
}

func NewProducer(streamUrl string) (*Producer, error) {
	redisWriter, err := NewRedisWriter(streamUrl)
	if err != nil {
		return nil, err
	}

	// Wrap in async writer so that we don't block the main thread
	return &Producer{writer: asyncwriter.New(redisWriter)}, nil
}

func (p *Producer) Write(data []byte) (int, error) {
	return p.writer.Write(data)
}

func (p *Producer) Flush() error {
	return p.writer.Flush()
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
