package server

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
	"wb-test/message_reciever/internal/app/repository"

	"github.com/gorilla/mux"
	"github.com/nats-io/stan.go"
)

// Server ...
type Server struct {
	router *mux.Router
	nats   stan.Conn
	repo   *repository.CouchbaseClient
}

// New ...
func New() *Server {
	return &Server{
		router: mux.NewRouter(),
		repo:   repository.New("couchbase://db", "Administrator", "password", "wb-test"),
	}
}

// Start ...
func (s *Server) Start() error {

	if err := s.repo.Connect(); err != nil {
		log.Println("couchbaseConnect error : ", err)
		return err
	}

	if err := s.natsConnect(); err != nil {
		log.Println("natsConnects error : ", err)
		return err
	}

	if err := http.ListenAndServe(":8080", s.router); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (s *Server) natsConnect() error {
	var err error
	s.nats, err = stan.Connect("test-cluster", "message_reciever", stan.NatsURL("nats://nats:4222"), stan.Pings(1, 2))
	if err != nil {
		return err
	}

	if err = s.NatsSubscribeOnInsertMessage(); err != nil {
		return err
	}

	if err = s.NatsSubscribeOnCountMessage(); err != nil {
		return err
	}

	return nil
}

// NatsSubscribeOnInsertMessage ...
func (s *Server) NatsSubscribeOnInsertMessage() error {
	var (
		data repository.Message
		err  error
	)

	_, err = s.nats.Subscribe("insert_messages", func(m *stan.Msg) {
		log.Printf("\n Received a message: %s\n", string(m.Data))
		defer m.Ack()

		err := json.Unmarshal(m.Data, &data)
		if err != nil {
			log.Println("NatsSubscribeOnInsertMessage natsInsertCh json.Unmarshal error:  ", err)
			return
		}
		//save to couchbase
		if err = s.repo.InsertMessage(&data); err != nil {
			log.Println("NatsSubscribeOnInsertMessage natsInsertCh InsertMessage error:  ", err)
			return
		}
		log.Printf("\n Message Inserted: %+v\n", data)
	}, stan.SetManualAckMode(), stan.AckWait(60*time.Second))
	if err != nil {
		log.Println("NatsSubscribeOnInsertMessage connect to subject - insert_messages  error : ", err)
		return err
	}
	return nil
}

// NatsSubscribeOnCountMessage ...
func (s *Server) NatsSubscribeOnCountMessage() error {

	var (
		message repository.Message
		data    struct {
			Count int `json:"count"`
		}
		err error
	)
	_, err = s.nats.Subscribe("get_message", func(m *stan.Msg) {
		log.Printf("\n Received a message: %s\n", string(m.Data))
		defer m.Ack()

		err := json.Unmarshal(m.Data, &message)
		if err != nil {
			log.Println("NatsSubscribeOnCountMessage json.Unmarshal error : ", err)
			return
		}

		//get from couchbase
		data.Count, err = s.repo.GetCountMessages(message.MessageType)
		if err != nil {
			log.Println("NatsSubscribeOnCountMessage GetCountMessages error : ", err)
			return
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			log.Println("NatsSubscribeOnCountMessage json.Marshal error : ", err)
			return
		}

		nuid, err := s.nats.PublishAsync("count_message", bytes, ackHandler)
		if err != nil {
			log.Printf("NatsSubscribeOnCountMessage error publishing msg %s: %v\n", nuid, err.Error())
			return
		}

	}, stan.SetManualAckMode(), stan.AckWait(60*time.Second))
	if err != nil {
		log.Println("NatsSubscribeOnCountMessage connect to subject - get_message error : ", err)
		return err
	}
	return nil
}

func ackHandler(ackedNuid string, err error) {
	if err != nil {
		log.Printf("Warning: error publishing msg id %s: %v\n", ackedNuid, err.Error())
	} else {
		log.Printf("Received ack for msg id %s\n", ackedNuid)
	}
}
