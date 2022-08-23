package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DirectExchange  ExchangeKind = "direct"
	FanoutExchange  ExchangeKind = "fanout"
	TopicExchange   ExchangeKind = "topic"
	HeadersExchange ExchangeKind = "headers"
)

type ExchangeKind string

type Exchange struct {
	Name string

	// Kind - "direct, "fanout", "topic" or "headers"
	Kind ExchangeKind

	// Staying declared, even after a broker restart
	Durable bool

	// Keeping even if it's not being consumed anymore
	AutoDeleted bool

	Internal bool

	NoWait bool

	Args amqp.Table
}

type Queue struct {
	Name string

	// Staying declared, even after a broker restart
	Durable bool

	// Delete when unused
	Delete bool

	Exclusive bool

	NoWait bool

	Args amqp.Table
}

type Message struct {
	ContentType string
	Body        []byte
}

type PubOptions struct {
	Key      string
	Exchange Exchange
	Message  Message
}

type SubOptions struct {
	Key      string
	Exchange Exchange
	Queue    Queue
}

type RabbitMQService struct {
	conn *amqp.Connection
}

func Open(address string) (*RabbitMQService, error) {
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, err
	}

	return &RabbitMQService{
		conn: conn,
	}, nil
}

// Opening new channel from the connection
func (r *RabbitMQService) openChannel() (*amqp.Channel, error) {
	return r.conn.Channel()
}

func (r *RabbitMQService) declareExchange(ex Exchange, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		ex.Name,
		string(ex.Kind),
		ex.Durable,
		ex.AutoDeleted,
		ex.Internal,
		ex.NoWait,
		ex.Args,
	)
}

func (r *RabbitMQService) declareQueue(queue Queue, channel *amqp.Channel) (amqp.Queue, error) {
	return channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.Delete,
		queue.Exclusive,
		queue.NoWait,
		queue.Args,
	)
}

func (r *RabbitMQService) bindQueue(options SubOptions, channel *amqp.Channel) (amqp.Queue, error) {
	queue, err := r.declareQueue(options.Queue, channel)
	if err != nil {
		return amqp.Queue{}, err
	}

	return queue, channel.QueueBind(
		queue.Name,
		options.Key,
		options.Exchange.Name,
		false,
		nil,
	)
}

func (r *RabbitMQService) Close() error {
	return r.conn.Close()
}

func (r *RabbitMQService) Publish(ctx context.Context, options PubOptions) error {
	channel, err := r.openChannel()
	if err != nil {
		return err
	}

	if err := r.declareExchange(options.Exchange, channel); err != nil {
		return err
	}

	return channel.PublishWithContext(
		ctx,
		options.Exchange.Name,
		options.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: options.Message.ContentType,
			Body:        options.Message.Body,
		},
	)
}

func (r *RabbitMQService) Subscribe(ctx context.Context, options SubOptions) (<-chan Message, error) {
	channel, err := r.openChannel()
	if err != nil {
		return nil, err
	}

	queue, err := r.bindQueue(options, channel)
	if err != nil {
		return nil, err
	}

	consume, err := channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal(err)
	}

	msg := make(chan Message)

	go func() {

		defer channel.Close()

		for c := range consume {

			m := Message{
				ContentType: "text/plain",
				Body:        []byte(c.Body),
			}

			msg <- m
		}

	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return msg, nil

}
