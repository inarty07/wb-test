package main

import (
	"log"

	"wb-test/message_sender/internal/app/server"
)

func main() {
	s := server.New()
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("Service waiting for message")
}
