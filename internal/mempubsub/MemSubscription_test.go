package mempubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/metadata"
)

func GetRandomSub() (*MemSubscription, error) {
	pjName := makeString(30)
	mb := NewMemBase()
	pjt, err := mb.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topicName := makeString(20)
	topicps := &pubsub.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicName),
	}
	err = pjt.CreateTopic(topicps)
	if err != nil {
		return nil, err
	}
	subName := makeString(20)
	subps := &pubsub.Subscription{
		Name:               fmt.Sprintf("projects/%s/subscriptions/%s", pjName, subName),
		AckDeadlineSeconds: 10,
	}
	topic := pjt.GetTopic(topicName)
	err = topic.CreateSub(subps)
	if err != nil {
		return nil, err
	}
	return topic.GetSub(subName).(*MemSubscription), nil
}

type TestingStreamingPullServer struct {
	sendChannel chan *pubsub.StreamingPullRequest
	recvChannel chan *pubsub.StreamingPullResponse
}

func (tsps *TestingStreamingPullServer) Send(data *pubsub.StreamingPullResponse) error {
	tsps.recvChannel <- data
	return nil
}
func (tsps *TestingStreamingPullServer) Recv() (*pubsub.StreamingPullRequest, error) {

	return <-tsps.sendChannel, nil
}

func (tsps *TestingStreamingPullServer) SetHeader(metadata.MD) error {
	return nil
}
func (tsps *TestingStreamingPullServer) SendHeader(metadata.MD) error {
	return nil
}
func (tsps *TestingStreamingPullServer) SetTrailer(metadata.MD) {}
func (tsps *TestingStreamingPullServer) Context() context.Context {
	return context.TODO()
}
func (tsps *TestingStreamingPullServer) SendMsg(m interface{}) error {
	return nil
}
func (tsps *TestingStreamingPullServer) RecvMsg(m interface{}) error {
	return nil
}

func TestBasicSubStream(t *testing.T) {
	ack_check_time = time.Millisecond * 5
	sub, err := GetRandomSub()
	assert.NoError(t, err)
	defer sub.topic.project.DeleteTopic(sub.topic.name)
	fm := &pubsub.StreamingPullRequest{
		MaxOutstandingMessages:   1,
		MaxOutstandingBytes:      1000000000,
		StreamAckDeadlineSeconds: 10,
	}
	tsps := &TestingStreamingPullServer{
		sendChannel: make(chan *pubsub.StreamingPullRequest),
		recvChannel: make(chan *pubsub.StreamingPullResponse),
	}
	ss := sub.CreateStreamingSubscription(fm, tsps)
	go ss.Run()
	msgUUID := uuid.NewString()
	data := []byte("TEST")
	sub.GetTopic().PublishMessage(&pubsub.PubsubMessage{MessageId: msgUUID, Data: data})
	rmsg := <-tsps.recvChannel
	assert.Equal(t, 1, len(rmsg.ReceivedMessages))
	assert.Equal(t, data, rmsg.ReceivedMessages[0].Message.Data)
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		AckIds: []string{rmsg.ReceivedMessages[0].AckId},
	}
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 0, len(sub.acks))
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		AckIds: []string{rmsg.ReceivedMessages[0].AckId},
	}
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 0, len(sub.acks))
}

func TestBasicManyMessages(t *testing.T) {
	ack_check_time = time.Millisecond * 5
	sub, err := GetRandomSub()
	assert.NoError(t, err)
	defer sub.topic.project.DeleteTopic(sub.topic.name)
	data := []byte("TEST")
	for i := 0; i < 10; i++ {
		sub.GetTopic().PublishMessage(&pubsub.PubsubMessage{MessageId: uuid.NewString(), Data: data})
	}
	fm := &pubsub.StreamingPullRequest{
		MaxOutstandingMessages:   15,
		MaxOutstandingBytes:      1000000000,
		StreamAckDeadlineSeconds: 10,
	}
	tsps := &TestingStreamingPullServer{
		sendChannel: make(chan *pubsub.StreamingPullRequest),
		recvChannel: make(chan *pubsub.StreamingPullResponse),
	}
	ss := sub.CreateStreamingSubscription(fm, tsps)
	go ss.Run()
	rmsg := <-tsps.recvChannel
	assert.Equal(t, 10, len(rmsg.ReceivedMessages))
	assert.Equal(t, data, rmsg.ReceivedMessages[0].Message.Data)
	assert.Equal(t, 10, len(sub.acks))
	for _, msg := range rmsg.ReceivedMessages {
		tsps.sendChannel <- &pubsub.StreamingPullRequest{
			AckIds: []string{msg.AckId},
		}
	}
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 0, len(sub.acks))
}

func TestBasicExpireAck(t *testing.T) {
	ack_check_time = time.Millisecond * 5
	sub, err := GetRandomSub()
	assert.NoError(t, err)
	defer sub.topic.project.DeleteTopic(sub.topic.name)
	fm := &pubsub.StreamingPullRequest{
		MaxOutstandingMessages:   1,
		MaxOutstandingBytes:      1000000000,
		StreamAckDeadlineSeconds: 10,
	}
	tsps := &TestingStreamingPullServer{
		sendChannel: make(chan *pubsub.StreamingPullRequest),
		recvChannel: make(chan *pubsub.StreamingPullResponse),
	}
	ss := sub.CreateStreamingSubscription(fm, tsps)
	go ss.Run()
	msgUUID := uuid.NewString()
	data := []byte("TEST")
	sub.GetTopic().PublishMessage(&pubsub.PubsubMessage{MessageId: msgUUID, Data: data})

	rmsg := <-tsps.recvChannel
	assert.Equal(t, 1, len(rmsg.ReceivedMessages))
	assert.Equal(t, data, rmsg.ReceivedMessages[0].Message.Data)
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		ModifyDeadlineSeconds: []int32{0},
		ModifyDeadlineAckIds:  []string{msgUUID},
	}
	time.Sleep(ack_check_time * 2)
	rmsg = <-tsps.recvChannel
	assert.Equal(t, 1, len(rmsg.ReceivedMessages))
	assert.Equal(t, data, rmsg.ReceivedMessages[0].Message.Data)
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		AckIds: []string{rmsg.ReceivedMessages[0].AckId},
	}
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		ModifyDeadlineSeconds: []int32{0},
		ModifyDeadlineAckIds:  []string{msgUUID},
	}
	rmsg = nil
	select {
	case rmsg = <-tsps.recvChannel:
	case <-time.After(time.Millisecond * 5):
	}
	assert.Nil(t, rmsg)
}
