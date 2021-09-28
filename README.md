# bMQ

BofaMQ

Install:

```
go get github.com/jordan-lumley/bmq
```

```

func main() {
	cfg := bmq.NodeConnectionConfig{
		NodeType:     bmq.NODE_CLIENT,
		NodeId:       "tester",
		RoutingKey:   "updatr",
		Url:          "amqp://test:test@repl-dev.round2pos.com/",
		ExchangeName: "updatr_exchange",
	}

	node, err := bmq.NewNode(cfg)
	if err != nil {
		log.Fatal(err)
	}

	node.SetMessageHandler(onMessage)

	go func() {
		for {
			time.Sleep(5 * time.Second)
			node.Send([]byte("Hello from client"))
		}
	}()

	engine := bmq.NewEngine()
	engine.AddNode(node)

	engine.Start()
}

func onMessage(message amqp.Delivery) {
	fmt.Println(string(message.Body))
	message.Ack(false)
}

```
