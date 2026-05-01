package queue

import (
	"fmt"

	"github.com/anovimous/orchestrator/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      string
}

type Message struct {
	JobId      string `json:"jobId"`
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

	err = channel.Qos(1, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set prefetch count: %w", err)
	}

	_, err = channel.QueueDeclare(
		cfg.Queue,
		true,
		false,
		false,
		false,
		nil)

	return &Consumer{
		connection: conn,
		channel:    channel,
		queue:      cfg.Queue,
	}, nil

}

func (c *Consumer) Consume() (<-chan amqp.Delivery, error) {

	messages, err := c.channel.Consume(
		c.queue, // queue name
		"",      // consumer tag, empty means auto generated
		false,   // auto ack, false means we ack manually
		false,   // exclusive
		false,   // no local
		false,   // no wait
		nil,     // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	return messages, nil

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
