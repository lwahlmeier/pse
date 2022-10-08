package fspubsub

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/metadata"
)

func GetRandomSub() (*FSSubscription, error) {
	dir, err := os.MkdirTemp("", "go-test")
	if err != nil {
		return nil, err
	}
	pjName := makeString(30)
	fsb, err := NewFSBase(dir)
	if err != nil {
		return nil, err
	}
	pjt, err := fsb.GetProject(pjName)
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
		Name: fmt.Sprintf("projects/%s/subscriptions/%s", pjName, subName),
	}
	topic := pjt.GetTopic(topicName)
	err = topic.CreateSub(subps)
	if err != nil {
		return nil, err
	}
	return topic.GetSub(subName).(*FSSubscription), nil
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
	return nil
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
	defer os.RemoveAll(sub.topic.project.fsBase.basePath)
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
	assert.Equal(t, 0, getAcks(sub.subPath))
	tsps.sendChannel <- &pubsub.StreamingPullRequest{
		AckIds: []string{rmsg.ReceivedMessages[0].AckId},
	}
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 0, getAcks(sub.subPath))
	sub.GetTopic().PublishMessage(&pubsub.PubsubMessage{MessageId: uuid.NewString(), Data: data})
	rmsg = <-tsps.recvChannel
	assert.Equal(t, 1, len(rmsg.ReceivedMessages))
	assert.Equal(t, data, rmsg.ReceivedMessages[0].Message.Data)
}

func TestBasicManyMessages(t *testing.T) {
	ack_check_time = time.Millisecond * 5
	sub, err := GetRandomSub()
	assert.NoError(t, err)
	defer os.RemoveAll(sub.topic.project.fsBase.basePath)
	data := []byte("TEST")
	for i := 0; i < 10; i++ {
		sub.GetTopic().PublishMessage(&pubsub.PubsubMessage{MessageId: uuid.NewString(), Data: data})
	}
	fm := &pubsub.StreamingPullRequest{
		MaxOutstandingMessages:   10,
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
	assert.Equal(t, 10, getAcks(sub.subPath))
	for _, msg := range rmsg.ReceivedMessages {
		tsps.sendChannel <- &pubsub.StreamingPullRequest{
			AckIds: []string{msg.AckId},
		}
	}
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, 0, getAcks(sub.subPath))
}

func getAcks(path string) int {
	fse, err := os.ReadDir(path)
	if err != nil {
		return -1
	}
	count := 0
	for _, fsi := range fse {
		if strings.HasSuffix(fsi.Name(), ".ack.proto") {
			count += 1
		}
	}
	return count
}

func TestBasicExpireAck(t *testing.T) {
	ack_check_time = time.Millisecond * 5
	sub, err := GetRandomSub()
	assert.NoError(t, err)
	defer os.RemoveAll(sub.topic.project.fsBase.basePath)
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
