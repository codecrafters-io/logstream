package main

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
)

func TestConsumerAndProducer(t *testing.T) {
	p, err := NewProducer(redisClient(t), "testKey")
	if err != nil {
		t.Fatalf("Create Producer Error: %v", err)
	}

	_, err = p.Write([]byte("Here's a "))
	if err != nil {
		t.Errorf("Produce Error: %v", err)
	}

	_, err = p.Write([]byte("string\n"))
	if err != nil {
		t.Errorf("Produce Error: %v", err)
	}

	p.Close()

	c, err := NewConsumer(redisClient(t), "testKey")
	bytes, err := ioutil.ReadAll(c)
	if err != nil {
		t.Errorf("Read Error: %v", err)
	}

	expected := "Here's a string\n"
	if string(bytes) != expected {
		t.Errorf("Expected %v, got: %v", expected, string(bytes))
	}
}

func TestLargeMessage(t *testing.T) {
	p, err := NewProducer(redisClient(t), "testKey2")
	if err != nil {
		t.Fatalf("Create Producer Error: %v", err)
	}

	longString := strings.Repeat("a", 10000)

	_, err = p.Write([]byte(longString))
	if err != nil {
		t.Errorf("Produce Error: %v", err)
	}

	p.Close()

	c, err := NewConsumer(redisClient(t), "testKey2")
	bytes, err := ioutil.ReadAll(c)
	if err != nil {
		t.Errorf("Read Error: %v", err)
	}

	if string(bytes) != longString {
		t.Errorf("Expected long string, got: %v", string(bytes))
	}
}

func redisClient(t *testing.T) *redis.Client {
	opts, err := redis.ParseURL("redis://localhost:6379/0")
	opts.DialTimeout = time.Second * 30
	if err != nil {
		t.Errorf("Err: %v", err)
		t.FailNow()
	}

	return redis.NewClient(opts)
}
