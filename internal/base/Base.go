package base

import (
	"time"

	"google.golang.org/genproto/googleapis/pubsub/v1"
)

var MAX_ACK_TIME = time.Second * 600

type BaseBackend interface {
	ParseProjectName(string) (string, error)
	ParseProjectAndTopicName(string) (string, string, error)
	ParseProjectAndSubscriptionName(string) (string, string, error)

	GetProject(string) (BaseProject, error)
	DeleteProject(string)
}

type BaseProject interface {
	GetBase() BaseBackend
	GetName() string
	CreateTopic(*pubsub.Topic) error
	GetTopic(string) BaseTopic
	DeleteTopic(string)
	GetAllTopics() map[string]BaseTopic
	GetAllSubscriptions() map[string]BaseSubscription
}

type BaseTopic interface {
	GetProject() BaseProject
	GetName() string
	CreateSub(*pubsub.Subscription) error
	GetSub(string) BaseSubscription
	DeleteSub(string)
	GetAllSubs() []BaseSubscription
	GetTopicPubSub() *pubsub.Topic
	PublishMessage(msg *pubsub.PubsubMessage) error
}

type BaseSubscription interface {
	GetTopic() BaseTopic
	GetName() string
	GetSubscriptionPubSub() *pubsub.Subscription
	UpdateAcks([]string, int32)
	UpdateAck(string, time.Duration)
	AckMessages([]string)
	GetMessages(maxMsgs int32, maxWait time.Duration) []*pubsub.ReceivedMessage
	CreateStreamingSubscription(firstRecvMsg *pubsub.StreamingPullRequest, streamingServer pubsub.Subscriber_StreamingPullServer) BaseStreamingSubcription
}

type BaseStreamingSubcription interface {
	Run()
}
