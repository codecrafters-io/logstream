package main

import (
	"fmt"
	"io"
)

type LimitedWriter struct {
	io.Writer
	Limit    int
	consumed int
	written  int
}

func (w *LimitedWriter) Write(p []byte) (n int, err error) {
	if w.consumed >= w.Limit {
		return len(p), nil
	}

	lim := len(p)

	if end := w.consumed + len(p); end > w.Limit {
		lim = w.Limit - w.consumed
	}

	w.consumed += len(p)

	if lim == 0 {
		return len(p), nil
	}

	n, err = w.Writer.Write(p[:lim])
	w.written += n

	return len(p), err
}

func (w *LimitedWriter) Close() (err error) {
	const MB = 1024 * 1024

	if w.consumed > w.Limit {
		_, _ = fmt.Fprintf(w.Writer, "\n---\nLogs exceeded limit of %.1f MB. %.1f MB truncated\n", float64(w.Limit)/MB, float64(w.consumed-w.Limit)/MB)
	}

	return nil
}
