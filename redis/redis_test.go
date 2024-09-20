package redis

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var redisUrl = os.Getenv("REDIS_URL")

func TestNewProducerConsumer(t *testing.T) {
	_, err := NewProducer("redis://somehost/1/streamkey")
	assert.NoError(t, err)

	_, err = NewConsumer("redis://somehost/1/streamkey")
	assert.NoError(t, err)
}

func TestProduceConsume(t *testing.T) {
	r, mock := redismock.NewClientMock()

	stream := "abcd"

	p, err := NewProducer("redis://somehost/1/streamkey")
	assert.NoError(t, err)

	c, err := NewConsumer("redis://somehost/1/streamkey")
	assert.NoError(t, err)

	if p, ok := p.(*RedisWriter); ok {
		p.client = &redisClient{client: r, stream: stream}
	} else {
		t.Fatal("p is not a RedisWriter")
	}

	if c, ok := c.(*Consumer); ok {
		c.redisClient = &redisClient{client: r, stream: stream}
	} else {
		t.Fatal("c is not a Consumer")
	}

	msgs := []string{"some message", "another message", "third"}

	// produce

	for _, msg := range msgs {
		mock.ExpectXAdd(&redis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: []string{"event_type", "log", "bytes", msg + "\n"},
		}).SetVal("OK")

		n, err := p.Write([]byte(msg + "\n"))
		assert.NoError(t, err)
		assert.Equal(t, len(msg)+1, n)
	}

	mock.ExpectXAdd(&redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: []string{"event_type", "disconnect"},
	}).SetVal("OK")

	err = p.Close()
	assert.NoError(t, err)

	// consume

	expected := strings.Join(msgs, "\n") + "\n"
	expMsgs := make([]redis.XMessage, len(msgs)+1)

	for i, msg := range msgs {
		expMsgs[i] = redis.XMessage{
			ID:     fmt.Sprintf("%d-1", i),
			Values: map[string]interface{}{"event_type": "log", "bytes": msg + "\n"},
		}
	}

	expMsgs[len(msgs)] = redis.XMessage{
		ID:     fmt.Sprintf("%d-1", len(msgs)),
		Values: map[string]interface{}{"event_type": "disconnect"},
	}

	mock.ExpectXRead(&redis.XReadArgs{
		Streams: []string{stream, "0"},
		Block:   5 * time.Second,
	}).SetVal([]redis.XStream{{
		Stream:   stream,
		Messages: expMsgs,
	}})

	var data bytes.Buffer
	bufsize := 5

	n, err := io.CopyBuffer(&data, c, make([]byte, bufsize))
	assert.NoError(t, err)
	assert.Equal(t, len(expected), int(n))

	assert.Equal(t, expected, data.String())

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProduceConsumeEnd2End(t *testing.T) {
	if redisUrl == "" {
		t.Fatal("no redis url provided")
	}

	streamKey := fmt.Sprintf("test-stream-%d", rand.Uint64())
	streamUrl := fmt.Sprintf("%s/%s", redisUrl, streamKey)

	p, err := NewProducer(streamUrl)
	require.NoError(t, err)

	c, err := NewConsumer(streamUrl)
	require.NoError(t, err)

	for _, msg := range []string{"first_message", "second_message"} {
		_, err = fmt.Fprintf(p, "%s\n", msg)
		assert.NoError(t, err)
	}

	err = p.Close()
	require.NoError(t, err)

	expected := "first_message\nsecond_message\n"
	data := bytes.NewBuffer([]byte{})

	_, err = io.Copy(data, c)
	assert.NoError(t, err)
	assert.Equal(t, []byte(expected), data.Bytes())
}
