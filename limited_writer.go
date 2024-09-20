package main

import (
	"fmt"
	"io"
)

const MB = 1024 * 1024

type LimitedWriter struct {
	Writer            io.Writer
	Limit             int
	consumed          int
	written           int
	hasWrittenWarning bool
}

func (w *LimitedWriter) Write(p []byte) (n int, err error) {
	if w.consumed >= w.Limit {
		w.consumed += len(p)
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

	if w.consumed > w.Limit {
		w.ensureExceededLimitWarningWritten()
	}

	return len(p), err
}

func (w *LimitedWriter) ensureExceededLimitWarningWritten() {
	if !w.hasWrittenWarning {
		_, _ = fmt.Fprintf(w.Writer, "\n---\nLogs exceeded limit of %.1f MB.\n", float64(w.Limit)/MB)
		w.hasWrittenWarning = true
	}

}

func (w *LimitedWriter) ensureTruncatedAmountWritten() {
	_, _ = fmt.Fprintf(w.Writer, "%.1f MB truncated\n", float64(w.consumed-w.Limit)/MB)
}

func (w *LimitedWriter) Close() (err error) {
	if w.consumed > w.Limit {
		w.ensureTruncatedAmountWritten()
	}

	if closer, ok := w.Writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
