package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/urfave/cli/v2"

	"github.com/codecrafters-io/logstream/redis"
)

var defaultSentryDSN = "https://ed4462d1f05d4473bcc3726fc2379c47@o294739.ingest.sentry.io/4504746068082688"

func main() {
	followCmd := &cli.Command{
		Name:   "follow",
		Action: follow,
	}

	runCmd := &cli.Command{
		Name:   "run",
		Action: run,
	}

	appendCmd := &cli.Command{
		Name:   "append",
		Action: appendRun,
	}

	app := &cli.App{
		Name: "logstream",
		Commands: []*cli.Command{
			followCmd,
			runCmd,
			appendCmd,
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Usage: "a logstream URL. Example: redis://localhost:6379/0/<stream_id>",
			},
			&cli.Float64Flag{
				Name:  "max-size-mbs",
				Value: 2,
				Usage: "max log size to stream, in MBs. Example: 2",
			},
			&cli.DurationFlag{
				Name:  "sentry-flush-timeout",
				Value: time.Second,
				Usage: "max time to wait for sentry events flush at exit",
			},
			cli.HelpFlag,
			cli.BashCompletionFlag,
		},
	}

	app.RunAndExitOnError()
}

func sentryInit(c *cli.Context, extra map[string]interface{}) func() {
	dsn := envOr("SENTRY_DSN", defaultSentryDSN)
	if dsn == "" {
		return func() {}
	}

	beforeSend := func(ev *sentry.Event, hint *sentry.EventHint) *sentry.Event {
		for k, v := range extra {
			ev.Extra[k] = v
		}

		return ev
	}

	opts := sentry.ClientOptions{
		Dsn:              dsn,
		Debug:            os.Getenv("SENTRY_DEBUG") == "1",
		TracesSampleRate: envFloatOr("SENTRY_SAMPLE_RATE", 1),
		Release:          "unknown",
		BeforeSend:       beforeSend,
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		opts.Release = info.Main.Version

		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				opts.Release += "-" + s.Value
			}
		}
	}

	err := sentry.Init(opts)
	_ = err // ignore

	return func() {
		to := c.Duration("sentry-flush-timeout")

		sentry.Flush(to)
	}
}

func sentryCatch(errptr *error) {
	if p := recover(); p != nil {
		sentry.CurrentHub().Recover(p)

		panic(p)
	}

	if *errptr == nil {
		return
	}

	err := *errptr

	var code *exec.ExitError
	if errors.As(err, &code) {
		return
	}

	sentry.CurrentHub().CaptureException(err)
}

func follow(c *cli.Context) (err error) {
	defer sentryInit(c, map[string]interface{}{
		"command": "follow",
		"redis":   c.String("url"),
	})()
	defer sentryCatch(&err)

	r, err := redis.NewConsumer(c.String("url"))
	if err != nil {
		return fmt.Errorf("new redis client: %w", err)
	}

	defer func() {
		e := r.Close()
		if err == nil && e != nil {
			err = fmt.Errorf("close redis: %w", e)
		}
	}()

	_, err = io.Copy(os.Stdout, r)
	if err != nil {
		return fmt.Errorf("read stream: %w", err)
	}

	return nil
}

func appendRun(c *cli.Context) (err error) {
	defer sentryInit(c, map[string]interface{}{
		"command": "append",
		"redis":   c.String("url"),
	})()
	defer sentryCatch(&err)

	r, err := redis.NewProducer(c.String("url"))
	if err != nil {
		return fmt.Errorf("new redis client: %w", err)
	}

	defer func() {
		e := r.Close()
		if err == nil && e != nil {
			err = fmt.Errorf("close redis: %w", e)
		}
	}()

	_, err = io.Copy(r, os.Stdin)
	if err != nil {
		return fmt.Errorf("write stream: %w", err)
	}

	return nil
}

func run(c *cli.Context) (err error) {
	defer sentryInit(c, map[string]interface{}{
		"command":      "run",
		"redis":        c.String("url"),
		"exec_command": c.Args().Slice(),
	})()
	defer sentryCatch(&err)

	r, err := redis.NewProducer(c.String("url"))
	if err != nil {
		return fmt.Errorf("new redis client: %w", err)
	}

	defer func() {
		e := r.Close()
		if err == nil && e != nil {
			err = fmt.Errorf("close redis: %w", e)
		}
	}()

	var producer io.Writer = r

	if lim := c.Float64("max-size-mbs"); lim != 0 {
		lw := &LimitedWriter{
			Writer: r,
			Limit:  int(lim * 1024 * 1024),
		}

		defer lw.Close()

		producer = lw
	}

	cmd := execBash(c)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("get stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("get stderr pipe: %w", err)
	}

	errc := make(chan error, 2)

	go copier("stdout", io.MultiWriter(producer, os.Stdout), stdout, errc)
	go copier("stderr", io.MultiWriter(producer, os.Stdout), stderr, errc)

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("start cmd: %w", err)
	}

	defer func() {
		e := cmd.Wait()
		if err == nil && e != nil {
			err = fmt.Errorf("wait for cmd: %w", e)
		}

		var code *exec.ExitError
		if errors.As(err, &code) {
			_, _ = fmt.Fprintf(r, "\n---\nCommand exit status: %v\n", code.ExitCode())
		}
	}()

	err = <-errc
	if err != nil {
		return
	}

	err = <-errc
	if err != nil {
		return
	}

	return nil
}

func copier(name string, w io.Writer, r io.Reader, errc chan error) {
	_, err := io.Copy(w, r)
	if err != nil {
		err = fmt.Errorf("%v: %w", name, err)
	}

	errc <- err
}

func execBash(c *cli.Context) *exec.Cmd {
	args := c.Args().Slice()

	for i := range args {
		args[i] = strconv.Quote(args[i])
	}

	return exec.Command("bash", "-c", strings.Join(args, " "))
}

func execNative(c *cli.Context) *exec.Cmd {
	return exec.Command(c.Args().First(), c.Args().Tail()...)
}

func envOr(name, defaultVal string) string {
	v, ok := os.LookupEnv(name)
	if ok {
		return v
	}

	return defaultVal
}

func envFloatOr(name string, defaultVal float64) float64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultVal
	}

	x, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultVal
	}

	return x
}
