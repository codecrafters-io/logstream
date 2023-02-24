package redis

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var redisFlag = flag.String("redis", "", "redis url")

func TestNewProducerConsumer(t *testing.T) {
	p, err := NewProducer("redis://somehost/1/streamkey")
	assert.NoError(t, err)
	assert.Equal(t, p.stream, "streamkey")

	c, err := NewConsumer("redis://somehost/1/streamkey")
	assert.NoError(t, err)
	assert.Equal(t, c.stream, "streamkey")
}

func TestProduceConsume(t *testing.T) {
	r, mock := redismock.NewClientMock()

	stream := "abcd"

	p := Producer{Redis: &Redis{client: r, stream: stream}}
	c := Consumer{Redis: &Redis{client: r, stream: stream}}

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

	err := p.Close()
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
		Streams: []string{stream, c.lastMessageID},
		Block:   5 * time.Second,
	}).SetVal([]redis.XStream{{
		Stream:   stream,
		Messages: expMsgs,
	}})

	var data bytes.Buffer
	bufsize := 5

	n, err := io.CopyBuffer(&data, &c, make([]byte, bufsize))
	assert.NoError(t, err)
	assert.Equal(t, len(expected), int(n))

	assert.Equal(t, expected, data.String())

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProduceConsumeEnd2End(t *testing.T) {
	if *redisFlag == "" {
		t.Skip("no redis url provided")
	}

	p, err := NewProducer(*redisFlag)
	require.NoError(t, err)

	c, err := NewConsumer(*redisFlag)
	require.NoError(t, err)

	for _, msg := range []string{"first_message", "second_message"} {
		_, err = fmt.Fprintf(p, "%s\n", msg)
		assert.NoError(t, err)
	}

	err = p.Close()
	require.NoError(t, err)

	expected := `first_message
second_message
`

	var data bytes.Buffer
	bufsize := 5

	_, err = io.CopyBuffer(&data,
		io.LimitReader(c, 1000),
		make([]byte, bufsize))
	assert.NoError(t, err)
	assert.Equal(t, []byte(expected), data.Bytes())
}
