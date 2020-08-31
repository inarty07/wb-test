package main

import (
	"log"

	"wb-test/message_reciever/internal/app/server"
)

func main() {
	s := server.New()
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("Service waiting for message")

}
