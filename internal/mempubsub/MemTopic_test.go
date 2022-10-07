package mempubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

func GetRandomTopic() (*MemTopic, error) {
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
	return pjt.GetTopic(topicName).(*MemTopic), nil
}

func TestMemTopicCreateBadSub(t *testing.T) {
	memTopic, err := GetRandomTopic()
	assert.NoError(t, err)
	assert.NotNil(t, memTopic)
	//Bad Name
	psSub := &pubsub.Subscription{Name: "BadName"}
	err = memTopic.CreateSub(psSub)
	assert.Error(t, err)
	psSub = &pubsub.Subscription{Name: fmt.Sprintf("projects/%s/subscriptions/%s", memTopic.project.name, makeString(20))}
	err = memTopic.CreateSub(psSub)
	assert.NoError(t, err)
	//Already exists
	err = memTopic.CreateSub(psSub)
	assert.Error(t, err)
}

func TestMemTopicGetSub(t *testing.T) {
	memTopic, err := GetRandomTopic()
	assert.NoError(t, err)
	assert.NotNil(t, memTopic)
	subName := makeString(20)
	sub := memTopic.GetSub(subName)
	assert.Nil(t, sub)
	psSub := &pubsub.Subscription{Name: fmt.Sprintf("projects/%s/subscriptions/%s", memTopic.project.name, subName)}
	err = memTopic.CreateSub(psSub)
	assert.NoError(t, err)
	sub = memTopic.GetSub(subName)
	assert.NotNil(t, sub)
	_, ok := memTopic.subs[subName]
	assert.True(t, ok)
	memTopic.DeleteSub(subName)
	_, ok = memTopic.subs[subName]
	assert.False(t, ok)
	//Make sure nothing happens
	memTopic.DeleteSub(subName)
}

func TestMemPublishTest(t *testing.T) {
	count := 100
	subs := make([]string, 0)
	memTopic, err := GetRandomTopic()
	assert.NoError(t, err)
	assert.NotNil(t, memTopic)
	for i := 0; i < count; i++ {
		subName := makeString(20)
		subs = append(subs, subName)
		psSub := &pubsub.Subscription{Name: fmt.Sprintf("projects/%s/subscriptions/%s", memTopic.project.name, subName)}
		err = memTopic.CreateSub(psSub)
		assert.NoError(t, err)
	}
	msgUUID := uuid.NewString()
	data := []byte("test")
	err = memTopic.PublishMessage(&pubsub.PubsubMessage{MessageId: msgUUID, Data: data})
	assert.NoError(t, err)
	msgUUID2 := uuid.NewString()
	data2 := []byte("test2")

	err = memTopic.PublishMessage(&pubsub.PubsubMessage{MessageId: msgUUID2, Data: data2})
	assert.NoError(t, err)

	for _, subName := range subs {
		sub := memTopic.GetSub(subName)
		msg := sub.GetMessages(3, time.Millisecond)

		sub.AckMessages([]string{msg[0].AckId, msg[1].AckId})
		assert.Equal(t, msgUUID, msg[0].AckId)
		assert.Equal(t, data, msg[0].Message.Data)
		assert.Equal(t, msgUUID2, msg[1].AckId)
		assert.Equal(t, data2, msg[1].Message.Data)
		assert.Equal(t, 2, len(msg))

	}
	for _, subName := range subs {
		sub := memTopic.GetSub(subName)
		msg := sub.GetMessages(1, time.Millisecond)
		assert.Equal(t, 0, len(msg))
	}
}
