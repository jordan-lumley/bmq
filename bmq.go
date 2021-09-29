package bmq

import "github.com/streadway/amqp"

type EventsHandler interface {
	OnMessage(message amqp.Delivery)
	OnError(err error)
}
