package bmq

import (
	"fmt"
	"net"
	"time"

	"github.com/streadway/amqp"
)

const (
	CLIENT BrokerType = iota
	SERVER BrokerType = iota
)

type (
	BrokerType int

	Broker struct {
		// broker id
		Id    string
		Type  BrokerType
		Route string

		// mq objects
		mqConnection   *amqp.Connection
		mqChannel      *amqp.Channel
		mqQueueName    string
		mqRoutingKey   string
		mqExchangeName string
		mqSendTo       string
	}

	Config struct {
		// connection settings
		Url     string
		Timeout time.Duration

		// identifiers
		Type BrokerType
		Id   string

		// mq settings
		Route        string
		ExchangeName string
	}
)

func NewBroker(config Config) (*Broker, error) {
	conn, err := amqp.DialConfig(config.Url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, config.Timeout)
		},
	})
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{
		Id:             config.Id,
		Type:           config.Type,
		Route:          config.Route,
		mqConnection:   conn,
		mqChannel:      ch,
		mqExchangeName: config.ExchangeName,
	}, nil
}

func (b *Broker) Send(data []byte) (err error) {
	err = b.mqChannel.Publish(
		b.mqExchangeName, // exchange
		b.mqSendTo,       // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})

	return
}

func (b *Broker) Start(handler EventsHandler) error {
	if b.Type == CLIENT {
		b.mqQueueName = fmt.Sprintf("%s-client", b.Id)
		b.mqRoutingKey = fmt.Sprintf("%s.server.%s", b.Id, b.Route)
		b.mqSendTo = fmt.Sprintf("%s.client.%s", b.Id, b.Route)
	} else {
		b.mqQueueName = fmt.Sprintf("%s-server", b.Id)
		b.mqRoutingKey = fmt.Sprintf("%s.client.%s", b.Id, b.Route)
		b.mqSendTo = fmt.Sprintf("%s.server.%s", b.Id, b.Route)
	}

	err := b.mqChannel.ExchangeDeclare(
		b.mqExchangeName, // name
		"direct",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return err
	}

	_, err = b.mqChannel.QueueDeclare(
		b.mqQueueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	err = b.mqChannel.QueueBind(
		b.mqQueueName,    // queue name
		b.mqRoutingKey,   // routing key
		b.mqExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	messages, err := b.mqChannel.Consume(
		b.mqQueueName, // queue
		"",            // consumer
		false,         // auto ack
		false,         // exclusive
		false,         // no local
		false,         // no wait
		nil,           // args
	)
	if err != nil {
		return err
	}

	for msg := range messages {
		handler.OnMessage(msg)
	}

	select {}
}
