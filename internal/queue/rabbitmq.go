package queue

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}
	return &RabbitMQ{conn: conn, ch: ch}, nil
}

func (r *RabbitMQ) Close() {
	r.ch.Close()
	r.conn.Close()
}

// DeclareQueue declara uma fila durável point-to-point.
func (r *RabbitMQ) DeclareQueue(name string) error {
	_, err := r.ch.QueueDeclare(name, true, false, false, false, nil)
	return err
}

// DeclareExchange declara um exchange fanout (pub/sub).
func (r *RabbitMQ) DeclareExchange(name string) error {
	return r.ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
}

// BindQueue cria uma fila exclusiva e a vincula ao exchange, retornando o nome da fila.
func (r *RabbitMQ) BindQueue(exchange string) (string, error) {
	q, err := r.ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return "", fmt.Errorf("declare exclusive queue: %w", err)
	}
	if err := r.ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		return "", fmt.Errorf("queue bind: %w", err)
	}
	return q.Name, nil
}

// Publish envia uma mensagem para a fila diretamente (default exchange).
func (r *RabbitMQ) Publish(ctx context.Context, queue string, payload []byte) error {
	return r.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	})
}

// Consume registra um consumer numa fila e devolve o canal de deliveries.
func (r *RabbitMQ) Consume(queue string) (<-chan amqp.Delivery, error) {
	return r.ch.Consume(queue, "", false, false, false, false, nil)
}
