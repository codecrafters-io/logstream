package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimitedWriter(t *testing.T) {
	var buf bytes.Buffer

	w := LimitedWriter{
		Writer: &buf,
		Limit:  10,
	}

	for i := 0; i < w.Limit; i += i + 1 {
		n, err := w.Write([]byte("message_long")[:i+1])
		assert.NoError(t, err)
		assert.Equal(t, i+1, n)
	}

	assert.Equal(t, `mmemessmes`, buf.String())

	err := w.Close()
	assert.NoError(t, err)

	assert.Contains(t, buf.String(), "\n---\nLogs exceeded limit")
}
