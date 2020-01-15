package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	streamUrl := flag.String("url", "", "A logstream URL. Example: redis://localhost:6379/0/<stream_id>")
	flag.Parse()

	if *streamUrl == "" {
		fmt.Println("Expected --url to be set!")
		os.Exit(1)
	}

	fmt.Printf("Streaming logs from/to %s\n", *streamUrl)
}
