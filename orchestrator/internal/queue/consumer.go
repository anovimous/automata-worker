package queue

import (
	"fmt"

	"github.com/anovimous/automata-worker/orchestrator/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	getter     getter
	queue      string
}

// getter is what Next needs of a channel, so that it can be replaced in tests.
type getter interface {
	Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error)
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
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-max-priority": int32(2),
		})
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &Consumer{
		connection: conn,
		channel:    channel,
		getter:     channel,
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
