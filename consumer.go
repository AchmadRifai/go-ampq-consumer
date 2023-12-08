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
	Args      ampq.Table
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
	msgs, err := channel.Consume(param.Queue, param.Consumer, param.AutoAck, param.Exclusive, param.NoLocal, param.NoWait, param.Args)
	if err != nil {
		panic(err)
	}
	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			defer normalError()
			process(msg)
		}
	}()
	<-forever
}

func killChannel(ch *ampq.Channel) {
	normalError()
	if ch != nil {
		if err := ch.Close(); err != nil {
			panic(err)
		}
	}
}

func killConnection(conn *ampq.Connection) {
	normalError()
	if conn != nil {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}
}

func normalError() {
	if r := recover(); r != nil {
		log.Println("Error catched", r)
		log.Println("Stack trace", string(debug.Stack()))
	}
}

type ExchangeDeclareParam struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       ampq.Table
}

type QueueDeclareParam struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       ampq.Table
}

type QueueBindParam struct {
	Name     string
	Key      string
	Exchange string
	NoWait   bool
	Args     ampq.Table
}
