package mempubsub

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

func TestMemProjectName(t *testing.T) {
	pjName := makeString(30)
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	assert.Equal(t, pjName, pjt.GetName())
}

func TestMemProjectGetBase(t *testing.T) {
	pjName := makeString(30)
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	assert.Equal(t, mb, pjt.GetBase())
}

func TestMemProjectCreateTopic(t *testing.T) {
	pjName := makeString(30)
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	topicName := makeString(20)
	topicps := &pubsub.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicName),
	}
	err = pjt.CreateTopic(topicps)
	assert.NoError(t, err)
	err = pjt.CreateTopic(topicps)
	assert.Error(t, err)
	topicps_bad := &pubsub.Topic{
		Name: fmt.Sprintf("projects/%s/top/%s", pjName, topicName),
	}
	err = pjt.CreateTopic(topicps_bad)
	assert.Error(t, err)
}

func TestMemProjectGetTopic(t *testing.T) {
	pjName := makeString(30)
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	topicName := makeString(20)
	topicps := &pubsub.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicName),
	}
	err = pjt.CreateTopic(topicps)
	assert.NoError(t, err)
	topic := pjt.GetTopic(topicName)
	assert.NotNil(t, topic)
	topic = pjt.GetTopic(makeString(20))
	assert.Nil(t, topic)
}

func TestMemProjectDeleteTopic(t *testing.T) {
	pjName := makeString(30)
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	topicName := makeString(20)
	topicps := &pubsub.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicName),
	}
	err = pjt.CreateTopic(topicps)
	assert.NoError(t, err)
	topic := pjt.GetTopic(topicName)
	assert.NotNil(t, topic)
	pjt.DeleteTopic(topicName)
	topic = pjt.GetTopic(topicName)
	assert.Nil(t, topic)
}

func TestMemProjectGetAllTopics(t *testing.T) {
	count := 100
	pjName := makeString(30)
	topicNames := make([]string, count)
	topics := make([]*pubsub.Topic, count)
	for i := 0; i < count; i++ {
		topicNames[i] = makeString(20)
		topics[i] = &pubsub.Topic{
			Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicNames[i]),
		}
	}

	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	assert.Equal(t, 0, len(pjt.GetAllTopics()))
	for i := 0; i < count; i++ {
		pjt.CreateTopic(topics[i])
	}
	assert.Equal(t, count, len(pjt.GetAllTopics()))
	for i := 0; i < count; i++ {
		topicName := topicNames[i]
		topic := pjt.GetTopic(topicName)
		assert.Equal(t, topicName, topic.GetName())
	}
	for i := 0; i < count; i++ {
		pjt.DeleteTopic(topicNames[i])
		assert.Equal(t, count-i-1, len(pjt.GetAllTopics()))
	}
}

func TestMemProjectGetAllSubs(t *testing.T) {
	count := 100
	pjName := makeString(30)
	topicNames := make([]string, count)
	topics := make([]*pubsub.Topic, count)
	subNames := make(map[string][]string)
	subs := make(map[string][]*pubsub.Subscription)
	for i := 0; i < count; i++ {
		topicNames[i] = makeString(20)
		topics[i] = &pubsub.Topic{
			Name: fmt.Sprintf("projects/%s/topics/%s", pjName, topicNames[i]),
		}
		subNames[topicNames[i]] = make([]string, count)
		subs[topicNames[i]] = make([]*pubsub.Subscription, count)
		for q := 0; q < count; q++ {
			subNames[topicNames[i]][q] = makeString(20)
			subs[topicNames[i]][q] = &pubsub.Subscription{
				Name:  fmt.Sprintf("projects/%s/subscriptions/%s", pjName, subNames[topicNames[i]][q]),
				Topic: topics[i].Name,
			}
		}
	}

	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.GetProject(pjName)
	assert.NoError(t, err)
	assert.NotNil(t, pjt)
	assert.Equal(t, 0, len(pjt.GetAllSubscriptions()))
	for i := 0; i < count; i++ {
		pjt.CreateTopic(topics[i])
		topic := pjt.GetTopic(topicNames[i])
		for q := 0; q < count; q++ {
			topic.CreateSub(subs[topicNames[i]][q])
		}
	}
	assert.Equal(t, count*count, len(pjt.GetAllSubscriptions()))
	for i := 0; i < count; i++ {
		pjt.DeleteTopic(topicNames[i])
		assert.Equal(t, count*(count-i-1), len(pjt.GetAllSubscriptions()))
	}
}
