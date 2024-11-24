package moneymakerrabbit

import (
	"context"
	json2 "encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
)

type Configuration struct {
	Host             string
	Port             string
	Username         string
	Password         string
	ConnectionPrefix string
}

type RabbitConnector struct {
	Connection *amqp091.Connection
}

type Connector interface {
	ReceiveMessages(queueName string, handler MessageHandlerFunc)
	SendMessage(body interface{}, headers map[string]interface{}, contentType string, queue string, exchange string) error
	Close()
	DeclareExchange(exchangeName string)
	DeclareQueue(queueName string) *amqp091.Queue
	ReceiveMessagesFromExchange(exchangeName string, consumingQueueName string, handler MessageHandlerFunc)
}

type MessageHandlerFunc func(msg *amqp091.Delivery)

func NewConfiguration() *Configuration {
	host := getOrDefault("RABBITMQ_HOST", "localhost")
	port := getOrDefault("RABBITMQ_PORT", "5672")
	username := getOrDefault("RABBITMQ_USERNAME", "test")
	password := getOrDefault("RABBITMQ_PASSWORD", "test")
	applicationName := getOrDefault("APPLICATION_NAME", "go-application")

	return &Configuration{
		Host:             host,
		Port:             port,
		Username:         username,
		Password:         password,
		ConnectionPrefix: applicationName,
	}
}

func (config *Configuration) Connect() Connector {

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", config.Username, config.Password, config.Host, config.Port)

	amqpConfig := amqp091.Config{Properties: map[string]interface{}{"connection_name": config.ConnectionPrefix}}

	conn, err := amqp091.DialConfig(url, amqpConfig)

	if err != nil {
		log.Panicf("%s: %s", "Failed to connect to RabbitMQ", err)
	}

	return &RabbitConnector{
		Connection: conn,
	}
}

// ReceiveMessages should be declared as a goroutine to ensure it does not block application startup
func (conn *RabbitConnector) ReceiveMessages(queueName string, handler MessageHandlerFunc) {

	ch := openChannel(conn.Connection)
	defer ch.Close()

	args := amqp091.Table{}
	args["x-message-ttl"] = 60000
	args["x-dead-letter-exchange"] = queueName + "-dlx"

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			handler(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages from queue %s\n", queueName)
	<-forever
}

// SendMessage should only take a queue or an exchange, not both
func (conn *RabbitConnector) SendMessage(body interface{}, headers map[string]interface{}, contentType string, queue string, exchange string) error {

	if queue != "" && exchange != "" {
		return fmt.Errorf("exchange and queue name cannot both be provided, one must be an empty string")
	}

	if queue == "" && exchange == "" {
		return fmt.Errorf("either a valid queue or exchange name must be provided, but were empty strings")
	}

	ch := openChannel(conn.Connection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	json, err := json2.Marshal(body)
	if err != nil {
		failOnError(err, "unable to marshal body to json")
	}

	err = ch.PublishWithContext(
		ctx,
		exchange,
		queue,
		false,
		false,
		amqp091.Publishing{
			ContentType: contentType,
			Headers:     headers,
			Body:        json,
		})

	if err != nil {
		return err
	}

	return nil
}

func (conn *RabbitConnector) Close() {
	err := conn.Connection.Close()
	if err != nil {
		log.Panicf("%s: %s", "Failed to close a channel", err)
	}
}

func (conn *RabbitConnector) DeclareExchange(exchangeName string) {
	ch := openChannel(conn.Connection)
	defer ch.Close()

	err := ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		log.Panicf("Failed to Declare Exchange with name %s: %s", exchangeName, err)
	}

}

func (conn *RabbitConnector) DeclareQueue(queueName string) *amqp091.Queue {
	ch := openChannel(conn.Connection)
	defer ch.Close()

	args := amqp091.Table{}
	args["x-message-ttl"] = 60000
	args["x-dead-letter-exchange"] = queueName + "-dlx"

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return &q
}

func (conn *RabbitConnector) ReceiveMessagesFromExchange(exchangeName string, consumingQueueName string, handler MessageHandlerFunc) {

	ch := openChannel(conn.Connection)
	defer ch.Close()

	err := ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, fmt.Sprintf("Failed to Declare Exchange with name %s: %s", exchangeName, err))

	args := amqp091.Table{}
	args["x-message-ttl"] = 60000
	args["x-dead-letter-exchange"] = consumingQueueName + "-dlx"

	q, err := ch.QueueDeclare(
		consumingQueueName, // name
		false,              // durable
		false,              // delete when unused
		true,               // exclusive
		false,              // no-wait
		args,               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	failOnError(err, "Unable to bind queue to exchange")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			handler(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages from exchange %s\n", exchangeName)
	<-forever
}

func openChannel(conn *amqp091.Connection) *amqp091.Channel {
	ch, err := conn.Channel()
	if err != nil {
		log.Panicf("%s: %s", "Failed to open a channel", err)
	}
	return ch
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func getOrDefault(envVar string, defaultVal string) string {
	val := os.Getenv(envVar)
	if val == "" {
		return defaultVal
	}
	return val
}
