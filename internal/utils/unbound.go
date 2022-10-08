package utils

import (
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

type DynamicUUIDChannel struct {
	data         []uuid.UUID
	inChannel    chan uuid.UUID
	outChannel   chan uuid.UUID
	countChannel chan int
	running      bool
}

func NewDynamicUUIDChannel() *DynamicUUIDChannel {
	duc := &DynamicUUIDChannel{
		data:         make([]uuid.UUID, 0, 10),
		inChannel:    make(chan uuid.UUID, 10),
		outChannel:   make(chan uuid.UUID, 10),
		countChannel: make(chan int),
		running:      true,
	}
	go duc.run()
	return duc
}

func (duc *DynamicUUIDChannel) Add() chan<- uuid.UUID {
	return duc.inChannel
}

func (duc *DynamicUUIDChannel) Get() <-chan uuid.UUID {
	return duc.outChannel
}

func (duc *DynamicUUIDChannel) Count() <-chan int {
	return duc.countChannel
}

func (duc *DynamicUUIDChannel) Stop() {
	duc.running = false
	select {
	case <-duc.Count():
	default:
	}
}

func (duc *DynamicUUIDChannel) run() {
	for duc.running {
		if len(duc.data) > 0 {
			select {
			case newMsg := <-duc.inChannel:
				duc.data = append(duc.data, newMsg)
			case duc.outChannel <- duc.data[0]:
				duc.data = duc.data[1:]
			case duc.countChannel <- len(duc.data) + len(duc.outChannel):
			}
		} else {
			select {
			case newStr := <-duc.inChannel:
				duc.data = append(duc.data, newStr)
			case duc.countChannel <- len(duc.data) + len(duc.outChannel):
			}
		}
	}
}

type DynamicMsgChannel struct {
	data         []*pubsub.ReceivedMessage
	inChannel    chan *pubsub.ReceivedMessage
	outChannel   chan *pubsub.ReceivedMessage
	countChannel chan int
	running      bool
}

func NewDynamicMsgChannel() *DynamicMsgChannel {
	dsc := &DynamicMsgChannel{
		data:         make([]*pubsub.ReceivedMessage, 0, 10),
		inChannel:    make(chan *pubsub.ReceivedMessage, 10),
		outChannel:   make(chan *pubsub.ReceivedMessage, 10),
		countChannel: make(chan int),
		running:      true,
	}
	go dsc.run()
	return dsc
}

func (dmc *DynamicMsgChannel) Add() chan<- *pubsub.ReceivedMessage {
	return dmc.inChannel
}

func (dmc *DynamicMsgChannel) Get() <-chan *pubsub.ReceivedMessage {
	return dmc.outChannel
}

func (dmc *DynamicMsgChannel) Count() <-chan int {
	return dmc.countChannel
}

func (dmc *DynamicMsgChannel) Stop() {
	dmc.running = false
	select {
	case <-dmc.Count():
	default:
	}
}

func (dmc *DynamicMsgChannel) run() {
	for dmc.running {
		if len(dmc.data) > 0 {
			select {
			case newMsg := <-dmc.inChannel:
				dmc.data = append(dmc.data, newMsg)
			case dmc.outChannel <- dmc.data[0]:
				dmc.data = dmc.data[1:]
			case dmc.countChannel <- len(dmc.data) + len(dmc.outChannel):
			}
		} else {
			select {
			case newStr := <-dmc.inChannel:
				dmc.data = append(dmc.data, newStr)
			case dmc.countChannel <- len(dmc.data) + len(dmc.outChannel):
			}
		}
	}
}
