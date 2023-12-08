package goampqconsumer

import (
	"log"
	"runtime/debug"

	ampq "github.com/rabbitmq/amqp091-go"
)

type ConsumerParams struct {
	Url     string
	Channel ChannelParam
}

type ChannelParam struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

func Consuming(param ConsumerParams, process func(ampq.Delivery)) {
	conn, err := ampq.Dial(param.Url)
	defer killConnection(conn)
	if err != nil {
		panic(err)
	}
	channelingConsuming(conn, param.Channel, process)
}

func channelingConsuming(conn *ampq.Connection, param ChannelParam, process func(ampq.Delivery)) {
	channel, err := conn.Channel()
	defer killChannel(channel)
	if err != nil {
		panic(err)
	}
	msgs, err := channel.Consume(param.Queue, param.Consumer, param.AutoAck, param.Exclusive, param.NoLocal, param.NoWait, nil)
	if err != nil {
		panic(err)
	}
	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			process(msg)
		}
	}()
	<-forever
}

func killChannel(ch *ampq.Channel) {
	if r := recover(); r != nil {
		log.Println("Error catched", r)
		log.Println("Stack trace", string(debug.Stack()))
	}
	if ch != nil {
		if err := ch.Close(); err != nil {
			panic(err)
		}
	}
}

func killConnection(conn *ampq.Connection) {
	if r := recover(); r != nil {
		log.Println("Error catched", r)
		log.Println("Stack trace", string(debug.Stack()))
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}
}
