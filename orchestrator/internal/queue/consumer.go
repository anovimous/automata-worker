package queue

import (
	"fmt"
	"sync"

	"github.com/anovimous/automata-worker/orchestrator/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

// The core declares the same queue (HttpJobRabbitMQConfig: durable, with
// x-max-priority 2) and the broker refuses a declaration that differs, which
// stops the orchestrator at startup. These must be kept equal to the core's.
const queueDurable = true

func queueArguments() amqp.Table {
	return amqp.Table{
		"x-max-priority": int32(2),
	}
}

type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	getter     getter
	closers    []closeNotifier
	queue      string
}

// getter is what Next needs of a channel, so that it can be replaced in tests.
type getter interface {
	Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error)
}

// closeNotifier is what NotifyClosed needs of the connection and of the channel.
type closeNotifier interface {
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
}

type Message struct {
	JobId      int    `json:"jobId"`
	RoutineKey string `json:"routineKey"`
}

func New(cfg config.RabbitMQConfig) (*Consumer, error) {

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	_, err = channel.QueueDeclare(
		cfg.Queue,
		queueDurable,
		false,
		false,
		false,
		queueArguments())
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &Consumer{
		connection: conn,
		channel:    channel,
		getter:     channel,
		closers:    []closeNotifier{conn, channel},
		queue:      cfg.Queue,
	}, nil

}

// Next takes one message from the queue, or reports ok false when it is empty.
// The messages are pulled one at a time, only when the orchestrator is free, so
// that a message does not wait in the buffer of a busy orchestrator while an idle
// one could run it. It has to be acked by the caller.
func (c *Consumer) Next() (msg amqp.Delivery, ok bool, err error) {

	msg, ok, err = c.getter.Get(c.queue, false)
	if err != nil {
		return amqp.Delivery{}, false, fmt.Errorf("failed to get a message from rabbitmq: %w", err)
	}

	return msg, ok, nil

}

// NotifyClosed returns a channel that gets the error each time the connection or the channel to RabbitMQ is closed
// by one, and is closed once both are closed (a Close by this side sends no error). Call it once, before Close.
func (c *Consumer) NotifyClosed() <-chan error {
	out := make(chan error, len(c.closers))
	var wg sync.WaitGroup
	for _, n := range c.closers {
		closed := n.NotifyClose(make(chan *amqp.Error, 1))
		wg.Go(func() {
			for err := range closed {
				out <- err
			}
		})
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (c *Consumer) Ack(msg amqp.Delivery) error {
	if err := msg.Ack(false); err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error {

	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %w", err)
	}

	if err := c.connection.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	return nil

}
