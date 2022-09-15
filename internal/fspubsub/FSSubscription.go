package fspubsub

import (
	"os"

	"google.golang.org/genproto/googleapis/pubsub/v1"
)

type FSSubscriptions struct {
	name               string
	topic              *FSTopic
	subPath            string
	pubsubSubscription *pubsub.Subscription
}

func (fss *FSSubscriptions) GetPubSubSubscription() *pubsub.Subscription {
	return fss.pubsubSubscription
}

func (fss *FSSubscriptions) UpdateAcks(ackIds []string, deadline int32) {

}

func (fss *FSSubscriptions) AckMessages(ackIds []string) {

}

func (fss *FSSubscriptions) GetMessages(max int32) []*pubsub.ReceivedMessage {
	return make([]*pubsub.ReceivedMessage, 0)
}

func (fss *FSSubscriptions) Delete() {
	os.RemoveAll(fss.subPath)
	fss.topic.RemoveSub(fss.name)
}
