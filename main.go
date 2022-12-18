package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/codecrafters-io/logstream/consumer"
)

var ctx = context.Background()

func main() {
	isDebug := os.Getenv("LOGSTREAM_DEBUG") == "true"

	logDebug := func(message string) {
		if isDebug {
			fmt.Println(message)
		}
	}

	streamUrl := flag.String("url", "", "A logstream URL. Example: redis://localhost:6379/0/<stream_id>")
	maxLogSizeMBPtr := flag.Int("max-size-mbs", 2, "Max log size to stream, in MBs. Example: 2")
	flag.Parse()

	if *streamUrl == "" {
		fmt.Println("Expected --url to be set!")
		os.Exit(1)
	}

	maxLogSizeMB := *maxLogSizeMBPtr
	maxLogSizeBytes := maxLogSizeMB * 1024 * 1024

	redisUrl, streamKey := parseUrl(*streamUrl)

	opts, err := redis.ParseURL(redisUrl)
	opts.DialTimeout = time.Second * 30
	if err != nil {
		fmt.Printf("Err: %v\n", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(opts)

	args := flag.Args()

	if args[0] == "follow" {
		logDebug("creating consumer")
		consumer, err := consumer.NewConsumer(*streamUrl, logDebug)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		logDebug("created consumer, initiating io.Copy")
		_, err = io.Copy(os.Stdout, consumer)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
	} else if args[0] == "run" {
		producer, err := NewProducer(redisClient, streamKey)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		for i := 1; i < len(args); i++ {
			args[i] = strconv.Quote(args[i])
		}

		cmd := exec.Command("sh", "-c", strings.Join(args[1:], " "))
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		readAckChan := make(chan error)

		go func() {
			n, err := io.CopyN(io.MultiWriter(producer, os.Stdout), stdout, int64(maxLogSizeBytes))
			if err == nil && n == int64(maxLogSizeBytes) {
				// We exhausted the byte count. EOF!
				readAckChan <- io.EOF
				io.Copy(ioutil.Discard, stdout) // If anything is remaining, drain
			} else if err == io.EOF {
				// We hit an EOF, this is a successful read.
				readAckChan <- nil
			} else {
				readAckChan <- err
			}
		}()

		go func() {
			n, err := io.CopyN(io.MultiWriter(producer, os.Stderr), stderr, int64(maxLogSizeBytes))
			if err == nil && n == int64(maxLogSizeBytes) {
				// We exhausted the byte count. EOF!
				readAckChan <- io.EOF
				io.Copy(ioutil.Discard, stderr) // If anything is remaining, drain
			} else if err == io.EOF {
				// We hit an EOF, this is a successful read.
				readAckChan <- nil
			} else {
				readAckChan <- err
			}
		}()

		err = cmd.Start()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
		for i := 0; i < 2; i++ {
			if err = <-readAckChan; err != nil {
				if err == io.EOF {
					producer.Write([]byte(fmt.Sprintf("\n---\nLogs exceeded limit of %dMB, might be truncated.\n---\n", maxLogSizeMB)))
				} else {
					producer.Write([]byte(fmt.Sprintf("\nError when reading logs: %v.\n", err)))
				}
			}
		}
		cmdErr := cmd.Wait()

		closeErr := producer.Close()
		if closeErr != nil {
			fmt.Printf("Close err: %v\n", closeErr)
			os.Exit(1)
		}

		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode()) // The program exited with a non-zero exit code
		} else if cmdErr != nil {
			fmt.Printf("Cmd Err: %v\n", cmdErr)
			os.Exit(1)
		}
	} else if args[0] == "append" {
		producer, err := NewProducer(redisClient, streamKey)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}

		_, err = io.Copy(producer, os.Stdin)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Invalid args! %v\n", args)
	}
}

type Producer struct {
	redisClient *redis.Client
	streamKey   string
}

func NewProducer(redisClient *redis.Client, streamKey string) (*Producer, error) {
	return &Producer{
		redisClient: redisClient,
		streamKey:   streamKey,
	}, nil
}

func (c *Producer) Write(p []byte) (int, error) {
	cmd := c.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: c.streamKey,
		ID:     "*", // Maybe we can do better than this?
		Values: map[string]interface{}{
			"event_type": "log",
			"bytes":      string(p),
		},
	})
	_, err := cmd.Result()
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (c *Producer) Close() error {
	cmd := c.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: c.streamKey,
		ID:     "*", // Maybe we can do better than this?
		Values: map[string]interface{}{
			"event_type": "disconnect",
		},
	})
	_, err := cmd.Result()
	return err
}

func parseUrl(streamUrl string) (redisUrl string, streamKey string) {
	parts := strings.Split(streamUrl, "/")
	streamKey = parts[len(parts)-1]
	redisUrl = strings.Join(parts[0:len(parts)-1], "/")
	return
}
