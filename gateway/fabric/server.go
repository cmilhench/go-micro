package fabric

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type Server struct {
	Name       string
	Exchange   string
	Addr       string
	Queue      string
	RoutingKey string

	handler Handler

	Properties map[string]string

	conn *amqp.Connection
}

func ListenAndServe(exchange, addr, queue, routingKey string, handler Handler) error {
	if handler == nil {
		//handler = DefaultServeMux
		return fmt.Errorf("handler is nil")
	}

	server := &Server{
		Name:       "https://github.com/cmilhench/micro/client",
		Exchange:   exchange,
		Addr:       addr,
		Queue:      queue,
		RoutingKey: routingKey,
		handler:    handler,
	}
	return server.ListenAndServe()
}

func (srv *Server) ListenAndServe() error {
	// Connect to the server.
	cfg := amqp.Config{
		Dial:       amqp.DefaultDial(time.Second * 10),
		Heartbeat:  60 * time.Second,
		Locale:     "en_US",
		Properties: amqp.Table{},
	}
	cfg.Properties["connection_name"] = srv.Name
	for k, v := range srv.Properties {
		cfg.Properties[k] = v
	}

	conn, err := amqp.DialConfig(srv.Addr, cfg)
	if err != nil {
		return fmt.Errorf("failed to dial amqp, %s", err)
	}
	srv.conn = conn
	notifyClose := make(chan *amqp.Error)
	conn.NotifyClose(notifyClose)
	//defer conn.Close()

	// Create a channel.
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel, %s", err)
	}
	//defer ch.Close()

	// Assign QoS controls.
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to assign QoS controls, %s", err)
	}

	// Declare the exchange.
	err = ch.ExchangeDeclare(
		srv.Exchange, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted when usused
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange, %s", err)
	}

	// Declare the queue.
	q, err := ch.QueueDeclare(
		srv.Queue, // name
		false,     // durable
		false,     // auto-delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue, %s", err)
	}

	// QueueBind
	err = ch.QueueBind(
		q.Name,         // queue name
		srv.RoutingKey, // routing key
		srv.Exchange,   // exchange
		false,
		nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue, %s", err)
	}

	// Listen for requests.
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer, %s", err)
	}
	// TODO: Multiplex to handle requests in parallel
	go func() {
		for {
			select {
			case msg := <-msgs:
				req := &Request{
					Exchange:   srv.Exchange,
					Topic:      msg.RoutingKey,
					RoutingKey: msg.RoutingKey,
					//URL:           srv.Addr,
					//CorrelationId: msg.CorrelationId,
					ContentType: msg.ContentType,
					Body:        msg.Body,
				}
				res := srv.handler(req)
				var body []byte
				if res != nil {
					body = res.Body
				}
				out := amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: msg.CorrelationId,
					Body:          body,
					Headers:       amqp.Table{},
				}
				for k, v := range res.Header {
					out.Headers[k] = v
				}
				err = ch.Publish(
					"",          // exchange
					msg.ReplyTo, // routing key
					false,       // mandatory
					false,       // immediate
					out)
				if err != nil {
					fmt.Printf("failed to reply, %s\n", err)
					continue
				}
			case err = <-notifyClose:
				// TODO: handle reconnect and rebind
				if err != nil {
					fmt.Printf("connection closed, %s\n", err)
				}
				return
			}
		}

	}()

	return nil
}

func (srv *Server) Shutdown() error {
	return srv.conn.Close()
}
