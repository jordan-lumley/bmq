package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jordan-lumley/bmq"
	"github.com/streadway/amqp"
)

type evtHandler struct {
	bmq.EventsHandler
}

func (evt *evtHandler) OnMessage(message amqp.Delivery) {
	fmt.Println(string(message.Body))

	message.Ack(false)
}

func main() {
	config := bmq.Config{
		Type: bmq.SERVER,
		Id:   "567",

		Route:        "test",
		ExchangeName: "test_exchange",

		Url:     "amqp://test:test@repl-dev.round2pos.com/",
		Timeout: 5 * time.Second,
	}

	broker, err := bmq.NewBroker(config)
	if err != nil {
		log.Fatal(err)
	}

	handler := new(evtHandler)
	go broker.Start(handler)

	go func() {
		for {
			time.Sleep(time.Second * 5)
			broker.Send([]byte("Hello from server!"))
		}
	}()

	select {}
}
