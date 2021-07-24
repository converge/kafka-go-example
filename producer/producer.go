package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/go-chi/chi"

	"github.com/Shopify/sarama"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

type JsonResponse struct {
	Success string   `json:"success"`
	Comment *Comment `json:"comment"`
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:9092", "localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	if err != nil {
		log.Println(err)
		return err
	}
	partition, offset, err := producer.SendMessage(msg)

	logMsg := &sarama.ProducerMessage{
		Topic: "log",
		Value: sarama.StringEncoder("msg logged!"),
	}
	partition, offset, err = producer.SendMessage(logMsg)
	if err != nil {
		log.Println(err)
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func CreateComment(w http.ResponseWriter, r *http.Request) {

	// convert body into bytes and send it to kafka
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	var comment Comment
	err = json.Unmarshal(data, &comment)
	if err != nil {
		log.Println(err)
	}

	PushCommentToQueue("comments", data)
	// Return Comment in JSON format
	var jsonResponse JsonResponse
	jsonResponse.Success = "true"
	jsonResponse.Comment = &comment

	jsonData, err := json.Marshal(jsonResponse)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jsonData))
}

func main() {
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/comment", CreateComment)
	})
	http.ListenAndServe(":3000", r)
}
