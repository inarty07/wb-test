#!/bin/sh
export GOOS=linux
export GOARCH=amd64
go build -o ./message_reciever/reciever ./message_reciever/cmd/reciever/main.go
go build -o ./message_sender/sender ./message_sender/cmd/sender/main.go

docker-compose up --build
