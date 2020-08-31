package server

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

// Server ...
type Server struct {
	router      *mux.Router
	nats        *nats.Conn
	natsCountCh chan *nats.Msg
}

// New ...
func New() *Server {
	return &Server{
		router:      mux.NewRouter(),
		natsCountCh: make(chan *nats.Msg, 64),
	}
}

// Start ...
func (s *Server) Start() error {
	var err error

	s.startRouter()

	s.nats, err = nats.Connect("nats:4222")
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

func (s *Server) handleInsertMessage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("Cannot ioutil.ReadAll error : ", err)
			return
		}
		if err = s.nats.Publish("insert_message", body); err != nil {
			log.Println("Cannot nats.Publish error : ", err)
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
		if err = s.nats.Publish("get_messages", body); err != nil {
			log.Println("handleCountMessagesByType nats.Publish error : ", err)
			return
		}

		sub, err := s.nats.ChanSubscribe("count_message", s.natsCountCh)
		if err != nil {
			return
		}
		defer sub.Unsubscribe()

		msg := <-s.natsCountCh

		w.Write(msg.Data)

	}
}
