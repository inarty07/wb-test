package repository

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gocb"
)

// CouchbaseClient ...
type CouchbaseClient struct {
	host       string
	username   string
	password   string
	bucketName string
	bucket     *gocb.Bucket
	cluster    *gocb.Cluster
}

// New ...
func New(host, username, password, bucketName string) *CouchbaseClient {
	return &CouchbaseClient{
		host:       host,
		username:   username,
		password:   password,
		bucketName: bucketName,
	}
}

// Connect ...
func (cb *CouchbaseClient) Connect() error {
	var err error

	cb.cluster, err = gocb.Connect(cb.host, gocb.ClusterOptions{
		Username: cb.username,
		Password: cb.password,
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: time.Minute,
		},
	})
	if err != nil {
		log.Fatalf("Error couchbase connect:  %v", err)
	}
	err = cb.cluster.WaitUntilReady(time.Minute, nil)
	if err != nil {
		log.Fatalf("Error cluster WaitUntilReady:  %v", err)
	} else {
		log.Println("\033[0;32m Couchbase Started! \033[0m")
	}

	cb.bucket = cb.cluster.Bucket("wb-test")
	if err = cb.bucket.WaitUntilReady(5*time.Second, nil); err != nil {
		log.Fatalf("Error WaitUntilReady:  %v", err)

	}
	return nil
}

// InsertMessage ...
func (cb *CouchbaseClient) InsertMessage(m *Message) error {

	_, err := cb.bucket.DefaultCollection().Insert(time.Now().String(), m, nil)
	if err != nil {
		log.Println("Error Insert to couchbase:  ", err)
		return err
	}
	return nil
}

// GetCountMessages ...
func (cb *CouchbaseClient) GetCountMessages(messageType int) (int, error) {

	fmt.Printf("get message with type %+v\n ", messageType)

	query := "SELECT x.* FROM `wb-test` x WHERE x.messageType = $1;"

	rows, err := cb.cluster.Query(query, &gocb.QueryOptions{PositionalParameters: []interface{}{messageType}})
	if err != nil {
		log.Println("GetCountMessages cluster.Query error : ", err)
		return 0, err
	}

	defer func() {
		if err = rows.Close(); err != nil {
			log.Println("GetCountMessages rows.Close error: ", err)
			return
		}
		err = rows.Err()
		if err != nil {
			log.Println("GetCountMessages rows.Error error : ", err)
			return
		}
	}()

	mess, counter := Message{}, 0

	for rows.Next() {
		if err = rows.Row(&mess); err != nil {
			log.Println("GetCountMessages rows.Row error : ", err)
			continue
		}
		if mess.Text != "" {
			counter++
		}

	}
	return counter, nil
}
