package main

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestConsumerAndProducer(t *testing.T) {
	p, err := NewProducer("redis://localhost:6379", "testKey")
	if err != nil {
		t.Errorf("Create Producer Error: %v", err)
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

	fmt.Println("Writing done.")

	c, err := NewConsumer("redis://localhost:6379", "testKey")
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
	p, err := NewProducer("redis://localhost:6379", "testKey")
	if err != nil {
		t.Errorf("Create Producer Error: %v", err)
	}

	longString := strings.Repeat("a", 10000)

	_, err = p.Write([]byte(longString))
	if err != nil {
		t.Errorf("Produce Error: %v", err)
	}

	p.Close()

	fmt.Println("Writing done.")

	c, err := NewConsumer("redis://localhost:6379", "testKey")
	bytes, err := ioutil.ReadAll(c)
	if err != nil {
		t.Errorf("Read Error: %v", err)
	}

	if string(bytes) != longString {
		t.Errorf("Expected long string, got: %v", string(bytes))
	}
}
