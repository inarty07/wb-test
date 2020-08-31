package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"wb-test/message_reciever/internal/app/repository"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

// Server ...
type Server struct {
	router       *mux.Router
	nats         *nats.Conn
	natsInsertCh chan *nats.Msg
	natsCountCh  chan *nats.Msg
	repo         *repository.CouchbaseClient
}

// New ...
func New() *Server {
	return &Server{
		router:       mux.NewRouter(),
		natsInsertCh: make(chan *nats.Msg, 64),
		natsCountCh:  make(chan *nats.Msg, 64),
		repo:         repository.New("couchbase://db", "Administrator", "password", "wb-test"),
	}
}

// Start ...
func (s *Server) Start() error {

	if err := s.natsConnect(); err != nil {
		log.Println("natsConnects error : ", err)
		return err
	}

	if err := s.repo.Connect(); err != nil {
		log.Println("couchbaseConnect error : ", err)
		return err
	}

	go s.watchNats()

	if err := http.ListenAndServe(":8080", s.router); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (s *Server) natsConnect() error {
	var err error
	s.nats, err = nats.Connect("nats:4222")
	if err != nil {
		return err
	}

	_, err = s.nats.ChanSubscribe("insert_message", s.natsInsertCh)
	if err != nil {
		return err
	}

	_, err = s.nats.ChanSubscribe("get_messages", s.natsCountCh)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) watchNats() {
	for {
		select {
		case msg := <-s.natsInsertCh:
			var data repository.Message
			err := json.Unmarshal(msg.Data, &data)
			if err != nil {
				log.Println("watchNats natsInsertCh json.Unmarshal error:  ", err)
				continue
			}
			//save to couchbase
			if err = s.repo.InsertMessage(&data); err != nil {
				log.Println("watchNats natsInsertCh InsertMessage error:  ", err)
				continue
			}
			fmt.Printf("Inserted: %+v\n", data)

		case msg := <-s.natsCountCh:
			var (
				m    repository.Message
				data struct {
					Count int `json:"count"`
				}
			)

			err := json.Unmarshal(msg.Data, &m)
			if err != nil {
				log.Println("watchNats natsCountCh json.Unmarshal error : ", err)
				return
			}

			//get from couchbase
			data.Count, err = s.repo.GetCountMessages(m.MessageType)
			if err != nil {
				log.Println("watchNats natsCountCh GetCountMessages error : ", err)
				return
			}

			bytes, err := json.Marshal(data)
			if err != nil {
				log.Println("watchNats natsCountCh json.Marshal error : ", err)
				return
			}

			err = s.nats.Publish("count_message", bytes)
			if err != nil {
				log.Println("watchNats natsCountCh nats.Publish error : ", err)
				return
			}
		}
	}
}
