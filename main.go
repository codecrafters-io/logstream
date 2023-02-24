package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/codecrafters-io/logstream/redis"
)

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
			cli.HelpFlag,
			cli.BashCompletionFlag,
		},
	}

	app.RunAndExitOnError()
}

func follow(c *cli.Context) (err error) {
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
	go copier("stderr", io.MultiWriter(producer, os.Stderr), stderr, errc)

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
