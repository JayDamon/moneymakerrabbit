package moneymakerrabbit

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"os"
)

type Configuration struct {
	Host             string
	Port             string
	Username         string
	Password         string
	ConnectionPrefix string
}

type Connection struct {
	Connection *amqp091.Connection
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

func (config *Configuration) Connect() *Connection {

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", config.Username, config.Password, config.Host, config.Port)

	amqpConfig := amqp091.Config{Properties: map[string]interface{}{"connection_name": config.ConnectionPrefix}}

	conn, err := amqp091.DialConfig(url, amqpConfig)

	if err != nil {
		log.Panicf("%s: %s", "Failed to connect to RabbitMQ", err)
	}

	return &Connection{
		Connection: conn,
	}
}

// ReceiveMessages should be declared as a goroutine to ensure it does not block application startup
func (conn *Connection) ReceiveMessages(queueName string, handler MessageHandlerFunc) {

	ch := openChannel(conn.Connection)
	defer ch.Close()

	_, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
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