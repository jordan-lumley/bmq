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

		QueueName  string
		RoutingKey string
		SendingTo  string

		// mq objects
		mqConnection   *amqp.Connection
		mqChannel      *amqp.Channel
		mqExchangeName string
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

	err = ch.ExchangeDeclare(
		config.ExchangeName, // name
		"direct",            // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return nil, err
	}

	broker := new(Broker)

	if config.Type == CLIENT {
		broker.QueueName = fmt.Sprintf("%s-client", config.Id)
		broker.RoutingKey = fmt.Sprintf("%s.server.%s", config.Id, config.Route)
		broker.SendingTo = fmt.Sprintf("%s.client.%s", config.Id, config.Route)
	} else {
		broker.QueueName = fmt.Sprintf("%s-server", config.Id)
		broker.RoutingKey = fmt.Sprintf("%s.client.%s", config.Id, config.Route)
		broker.SendingTo = fmt.Sprintf("%s.server.%s", config.Id, config.Route)
	}

	broker.Id = config.Id
	broker.Type = config.Type
	broker.Route = config.Route
	broker.mqChannel = ch
	broker.mqConnection = conn
	broker.mqExchangeName = config.ExchangeName

	return broker, nil
}

func (b *Broker) Send(data []byte) (err error) {
	err = b.mqChannel.Publish(
		b.mqExchangeName, // exchange
		b.SendingTo,      // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})

	return
}

func (b *Broker) Close() {
	defer b.mqChannel.Close()
	defer b.mqConnection.Close()
}

func (b *Broker) Start(handler EventsHandler) error {
	_, err := b.mqChannel.QueueDeclare(
		b.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}

	err = b.mqChannel.QueueBind(
		b.QueueName,      // queue name
		b.RoutingKey,     // routing key
		b.mqExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	messages, err := b.mqChannel.Consume(
		b.QueueName, // queue
		"",          // consumer
		false,       // auto ack
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // args
	)
	if err != nil {
		return err
	}

	for msg := range messages {
		handler.OnMessage(msg)
	}

	select {}
}
