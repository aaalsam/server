package streaming

import (
	"log"
	"pkg/storage"

	"github.com/nats-io/stan.go"
)

type StreamingHandler struct {
	conn  *stan.Conn
	sub   *Subscriber
	name  string
	isErr bool
}

func NewStreamingHandler(st *storage.Storage) *StreamingHandler {
	sh := StreamingHandler{}
	sh.Init(st)
	return &sh
}

func (sh *StreamingHandler) Init(st *storage.Storage) {
	sh.name = "StreamingHandler"
	err := sh.Connect()

	if err != nil {
		sh.isErr = true
		log.Printf("%s: StreamingHandler ошибка: %s", sh.name, err)
	} else {
		sh.sub = NewSubscriber(st, sh.conn)
		sh.sub.Subscriber()
	}
}

func (sh *StreamingHandler) Connect() error {
	conn, err := stan.Connect("test-cluster", "nastyaM")
	if err != nil {
		log.Printf("%s: невозможно подключиться: %v.\n", sh.name, err)
		return err
	}
	sh.conn = &conn

	log.Printf("%s: подключенно", sh.name)
	return nil
}

func (sh *StreamingHandler) Finish() {
	if !sh.isErr {
		log.Printf("%s: Завершение...", sh.name)
		sh.sub.Unsubscribe() 
		(*sh.conn).Close()
		log.Printf("%s: Завершенно", sh.name)
	}
}
