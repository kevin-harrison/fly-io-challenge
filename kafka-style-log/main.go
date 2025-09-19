package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	server := NewServer(n)
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
