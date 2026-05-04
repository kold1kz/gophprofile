package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gophprofile/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// RoutingAvatarUploaded используется для событий о новой загрузке аватара.
	RoutingAvatarUploaded = "avatar.uploaded"

	// RoutingAvatarDeleted используется для событий об удалении аватара и связанных файлов.
	RoutingAvatarDeleted = "avatar.deleted"

	confirmTimeout = 5 * time.Second
)

// RabbitMQ инкапсулирует подключение к брокеру, exchange, рабочую очередь и DLQ.
type RabbitMQ struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	confirms   <-chan amqp.Confirmation
	returns    <-chan amqp.Return
	exchange   string
	queue      string
	dlx        string
	deadLetter string
}

// NewRabbitMQ подключается к RabbitMQ и объявляет все сущности, нужные для надежной доставки.
func NewRabbitMQ(url, exchange, queue string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	r := &RabbitMQ{
		conn:       conn,
		channel:    ch,
		exchange:   exchange,
		queue:      queue,
		dlx:        exchange + ".dlx",
		deadLetter: queue + ".dlq",
	}
	if err := r.declare(); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	r.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	r.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	return r, nil
}

func (r *RabbitMQ) declare() error {
	if err := r.channel.ExchangeDeclare(r.exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := r.channel.ExchangeDeclare(r.dlx, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	_, err := r.channel.QueueDeclare(r.deadLetter, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := r.channel.QueueBind(r.deadLetter, "#", r.dlx, false, nil); err != nil {
		return err
	}

	_, err = r.channel.QueueDeclare(r.queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": r.dlx,
	})
	if err != nil {
		return err
	}
	for _, key := range []string{RoutingAvatarUploaded, RoutingAvatarDeleted} {
		if err := r.channel.QueueBind(r.queue, key, r.exchange, false, nil); err != nil {
			return err
		}
	}
	return r.channel.Qos(1, 0, false)
}

// PublishUpload публикует событие о новой загрузке и ждет confirm от брокера.
func (r *RabbitMQ) PublishUpload(ctx context.Context, event domain.AvatarUploadEvent) error {
	return r.publish(ctx, RoutingAvatarUploaded, event.MessageID, event)
}

// PublishDelete публикует событие удаления и ждет confirm от брокера.
func (r *RabbitMQ) PublishDelete(ctx context.Context, event domain.AvatarDeleteEvent) error {
	return r.publish(ctx, RoutingAvatarDeleted, event.MessageID, event)
}

// Consume подписывает worker на очередь в режиме manual ack.
func (r *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	return r.channel.Consume(r.queue, "", false, false, false, false, nil)
}

// Ping проверяет, что exchange существует и канал RabbitMQ жив.
func (r *RabbitMQ) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return r.channel.ExchangeDeclarePassive(r.exchange, "topic", true, false, false, false, nil)
}

// Close закрывает канал и соединение с RabbitMQ.
func (r *RabbitMQ) Close() error {
	if r.channel != nil {
		_ = r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *RabbitMQ) publish(ctx context.Context, routingKey, messageID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, confirmTimeout)
	defer cancel()

	if err := r.channel.PublishWithContext(ctx, r.exchange, routingKey, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	}); err != nil {
		return err
	}

	select {
	case ret := <-r.returns:
		return fmt.Errorf("rabbitmq returned message %q: %s", ret.RoutingKey, ret.ReplyText)
	case confirmation := <-r.confirms:
		if confirmation.Ack {
			return nil
		}
		return errors.New("rabbitmq rejected publish")
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq publish confirm timeout: %w", ctx.Err())
	}
}
