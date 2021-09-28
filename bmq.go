package bmq

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/streadway/amqp"
)

type NodeType int

const (
	NODE_CLIENT NodeType = iota
	NODE_SERVER NodeType = iota
)

type Node struct {
	NodeId       string
	QueueName    string
	RoutingKey   string
	SendTo       string
	ExchangeName string

	MQConnection     *amqp.Connection
	MQChannel        *amqp.Channel
	OnMessageHandler func(amqp.Delivery)
}

type NodeConnectionConfig struct {
	NodeType NodeType
	NodeId   string

	RoutingKey   string
	Url          string
	ExchangeName string
}

type Engine struct {
	Nodes []*Node
}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) AddNode(node *Node) {
	e.Nodes = append(e.Nodes, node)
}

func (e *Engine) Start() {
	for _, node := range e.Nodes {
		go func(node *Node) {
			defer node.MQConnection.Close()
			defer node.MQChannel.Close()

			messages, err := node.subscribe()
			if err != nil {
				log.Fatal(err)
			}

			for msg := range messages {
				node.OnMessageHandler(msg)
			}
		}(node)
	}

	forever := make(chan bool)
	<-forever
}

func NewNode(config NodeConnectionConfig) (*Node, error) {
	var queueName string
	var routingKey string
	var sendTo string
	if config.NodeType == NODE_CLIENT {
		queueName = fmt.Sprintf("%s-client", config.NodeId)
		routingKey = fmt.Sprintf("%s.server.%s", config.NodeId, config.RoutingKey)
		sendTo = fmt.Sprintf("%s.client.%s", config.NodeId, config.RoutingKey)
	} else {
		queueName = fmt.Sprintf("%s-server", config.NodeId)
		routingKey = fmt.Sprintf("%s.client.%s", config.NodeId, config.RoutingKey)
		sendTo = fmt.Sprintf("%s.server.%s", config.NodeId, config.RoutingKey)
	}

	conn, err := amqp.DialConfig(config.Url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 10*time.Second)
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

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		queueName,           // queue name
		routingKey,          // routing key
		config.ExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Node{
		QueueName:    queueName,
		RoutingKey:   routingKey,
		SendTo:       sendTo,
		NodeId:       config.NodeId,
		MQChannel:    ch,
		MQConnection: conn,
		ExchangeName: config.ExchangeName,
	}, nil
}

func (n *Node) SetMessageHandler(handler func(amqp.Delivery)) {
	n.OnMessageHandler = handler
}

func (n *Node) subscribe() (<-chan amqp.Delivery, error) {
	msgs, err := n.MQChannel.Consume(
		n.QueueName, // queue
		"",          // consumer
		false,       // auto ack
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // args
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func (n *Node) Send(data []byte) error {
	err := n.MQChannel.Publish(
		n.ExchangeName, // exchange
		n.SendTo,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(string(data)),
		})
	if err != nil {
		return err
	}

	return nil
}
