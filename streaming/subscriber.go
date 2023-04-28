package streaming

import (
	"encoding/json"
	"log"
	"pkg/storage"

	"github.com/nats-io/stan.go"
)

type Subscriber struct {
	sub  stan.Subscription
	st   *storage.Storage
	name string
	sc   *stan.Conn
}

type Client struct {
	Id     int    `json:"id"`
	Name   string `json:"name"`
	Number int    `json:"number"`
}

func NewSubscriber(st *storage.Storage, conn *stan.Conn) *Subscriber {
	return &Subscriber{
		name: "Subscriber",
		st:   st,
		sc:   conn,
	}
}

func (s *Subscriber) Subscriber() {
	var err error
	subject := "clients"

	s.sub, err = (*s.sc).Subscribe(
		subject,
		func(m *stan.Msg) {
			log.Printf("%s: сообщение получено!\n", s.name)
			if s.messageHandler(m.Data) {
				err := m.Ack()
				if err != nil {
					log.Printf("%s ack() err: %s", s.name, err)
				}
			}
		},
		stan.SetManualAckMode())
	if err != nil {
		log.Printf("%s: ошибка: %v\n", s.name, err)
	}
	log.Printf("%s: подписка на тему %s\n", s.name, subject)
}

func (s *Subscriber) messageHandler(data []byte) bool {
	recievedClient := storage.Client{}
	err := json.Unmarshal(data, &recievedClient)
	if err != nil {
		log.Printf("%s: ошибка обработки сообщения, %v\n", s.name, err)
		return true
	}
	log.Printf("%s: данные переданны: %v\n", s.name, recievedClient)

	_, err = s.st.AddClient(recievedClient)
	if err != nil {
		log.Printf("%s: невозможно добавить клиента: %v\n", s.name, err)
		return false
	}
	return true
}

func (s *Subscriber) Unsubscribe() {
	if s.sub != nil {
		s.sub.Unsubscribe()
	}
}
