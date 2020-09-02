package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/stan.go"
)

// Server ...
type Server struct {
	router      *mux.Router
	nats        stan.Conn
	natsCountCh chan []byte
}

// New ...
func New() *Server {
	return &Server{
		router:      mux.NewRouter(),
		natsCountCh: make(chan []byte, 64),
	}
}

// Start ...
func (s *Server) Start() error {
	var err error

	s.startRouter()

	s.nats, err = stan.Connect("test-cluster", "message_sender", stan.NatsURL("nats:4222"), stan.Pings(1, 2))
	if err != nil {
		log.Println("Nats connect error : ", err)
		return err
	}

	if err = http.ListenAndServe(":8000", s.router); err != nil {
		log.Fatal(err)
	}

	return err
}

func (s *Server) startRouter() {
	s.router.HandleFunc("/save", s.handleInsertMessage()).Methods("PUT")
	s.router.HandleFunc("/getCount", s.handleCountMessagesByType()).Methods("GET")

}

func ackHandler(ackedNuid string, err error) {
	if err != nil {
		log.Printf("Warning: error publishing msg id %s: %v\n", ackedNuid, err.Error())
	} else {
		log.Printf("Received ack for msg id %s\n", ackedNuid)
	}
}

func (s *Server) handleInsertMessage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("handleInsertMessage Cannot ioutil.ReadAll error : ", err)
			return
		}

		fmt.Printf("\n Send Message To NATS: %+v\n", string(body))

		nuid, err := s.nats.PublishAsync("insert_messages", body, ackHandler) // returns immediately
		if err != nil {
			log.Printf("handleInsertMessage Error publishing msg %s: %v\n", nuid, err.Error())
			return
		}

		w.Write([]byte("Success"))
	}
}

func (s *Server) handleCountMessagesByType() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("handleCountMessagesByType ioutil.ReadAll error : ", err)
			return
		}
		if nuid, err := s.nats.PublishAsync("get_message", body, ackHandler); err != nil {
			log.Printf("handleCountMessagesByType Error publishing msg %s: %v\n", nuid, err.Error())
			return
		}

		sub, err := s.nats.Subscribe("count_message", func(m *stan.Msg) {
			defer m.Ack()
			s.natsCountCh <- m.Data
		}, stan.SetManualAckMode(), stan.AckWait(60*time.Second))
		if err != nil {
			return
		}
		defer sub.Unsubscribe()

		msg := <-s.natsCountCh

		w.Write(msg)

	}
}
