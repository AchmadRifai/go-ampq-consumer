package goampqconsumer

import (
	ampq "github.com/rabbitmq/amqp091-go"
)

type ConsumerPipeline struct {
	url                   string
	channel               *ampq.Channel
	channelParam          ChannelParam
	exchangeDeclareParams []ExchangeDeclareParam
	queueDeclareParams    []QueueDeclareParam
	queueBindParams       []QueueBindParam
}

func (p *ConsumerPipeline) GetChannel() *ampq.Channel { return p.channel }

func (p *ConsumerPipeline) ExchangeDeclare(param ExchangeDeclareParam) *ConsumerPipeline {
	p.exchangeDeclareParams = append(p.exchangeDeclareParams, param)
	return p
}

func (p *ConsumerPipeline) QueueDeclare(param QueueDeclareParam) *ConsumerPipeline {
	p.queueDeclareParams = append(p.queueDeclareParams, param)
	return p
}

func (p *ConsumerPipeline) QueueBind(param QueueBindParam) *ConsumerPipeline {
	p.queueBindParams = append(p.queueBindParams, param)
	return p
}

func (p *ConsumerPipeline) setup(ch *ampq.Channel) {
	for _, param := range p.exchangeDeclareParams {
		err := ch.ExchangeDeclare(param.Name, param.Kind, param.Durable, param.AutoDelete, param.Internal, param.NoWait, param.Args)
		if err != nil {
			panic(err)
		}
	}
	for _, param := range p.queueDeclareParams {
		_, err := ch.QueueDeclare(param.Name, param.Durable, param.AutoDelete, param.Exclusive, param.NoWait, param.Args)
		if err != nil {
			panic(err)
		}
	}
	for _, param := range p.queueBindParams {
		err := ch.QueueBind(param.Name, param.Key, param.Exchange, param.NoWait, param.Args)
		if err != nil {
			panic(err)
		}
	}
}

func (p *ConsumerPipeline) Execute(process func(ampq.Delivery)) {
	conn, err := ampq.Dial(p.url)
	defer killConnection(conn)
	if err != nil {
		panic(err)
	}
	p.channel, err = conn.Channel()
	defer killChannel(p.channel)
	if err != nil {
		panic(err)
	}
	p.setup(p.channel)
	param := p.channelParam
	msgs, err := p.channel.Consume(param.Queue, param.Consumer, param.AutoAck, param.Exclusive, param.NoLocal, param.NoWait, param.Args)
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

func NewConsumerPipeline(url string, channelParam ChannelParam) *ConsumerPipeline {
	return &ConsumerPipeline{url: url, channelParam: channelParam}
}
